# -*- coding: utf-8 -*-
"""
Cloud history sync service.

Download database artifacts from GitHub Actions and merge them into local SQLite
history tables so WebUI can show cloud-generated analysis records.
"""

from __future__ import annotations

import json
import logging
import os
import re
import shutil
import sqlite3
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

logger = logging.getLogger(__name__)

DEFAULT_WORKFLOWS: Tuple[str, ...] = ("daily_analysis.yml", "deep_analysis.yml")
DEFAULT_ARTIFACT_NAME = "analysis-db"
DEFAULT_LOOKBACK_RUNS = 20
DEFAULT_TIMEOUT_SECONDS = 20
STATE_FILE_NAME = "cloud_history_sync_state.json"
STATE_MAX_RUN_IDS = 2000
SYNC_TABLES: Tuple[str, ...] = ("analysis_history", "news_intel", "fundamental_snapshot")


def sync_cloud_history_from_github_actions(local_db_path: Optional[str] = None) -> Dict[str, Any]:
    """
    Pull cloud DB artifacts from GitHub Actions and merge into local DB.

    Returns:
        Dict summary with status and merged row counts.
    """
    summary: Dict[str, Any] = {
        "status": "skipped",
        "reason": "",
        "processed_runs": 0,
        "merged_rows": 0,
        "rows_by_table": {name: 0 for name in SYNC_TABLES},
        "synced_codes": [],
    }

    if os.getenv("GITHUB_ACTIONS") == "true":
        summary["reason"] = "running_in_github_actions"
        return summary

    if not _env_bool("CLOUD_HISTORY_SYNC_ENABLED", default=True):
        summary["reason"] = "disabled_by_env"
        return summary

    if shutil.which("gh") is None:
        summary["reason"] = "gh_cli_not_found"
        logger.info("[CloudSync] skip: GitHub CLI (gh) not found.")
        return summary

    repo = _resolve_repo_name()
    if not repo:
        summary["reason"] = "repo_not_resolved"
        logger.info("[CloudSync] skip: cannot resolve GitHub repo, set CLOUD_HISTORY_SYNC_REPO=owner/repo.")
        return summary

    workflows = _parse_workflows(os.getenv("CLOUD_HISTORY_SYNC_WORKFLOWS", ""))
    if not workflows:
        workflows = list(DEFAULT_WORKFLOWS)

    artifact_name = (os.getenv("CLOUD_HISTORY_SYNC_ARTIFACT_NAME", DEFAULT_ARTIFACT_NAME) or "").strip()
    if not artifact_name:
        artifact_name = DEFAULT_ARTIFACT_NAME

    lookback_runs = _env_int("CLOUD_HISTORY_SYNC_LOOKBACK_RUNS", DEFAULT_LOOKBACK_RUNS, minimum=1, maximum=200)
    timeout_seconds = _env_int("CLOUD_HISTORY_SYNC_GH_TIMEOUT_SECONDS", DEFAULT_TIMEOUT_SECONDS, minimum=5, maximum=180)

    destination_db = Path(local_db_path or os.getenv("DATABASE_PATH", "./data/stock_analysis.db")).resolve()
    destination_db.parent.mkdir(parents=True, exist_ok=True)
    state_file = Path(
        os.getenv("CLOUD_HISTORY_SYNC_STATE_FILE", str(destination_db.parent / STATE_FILE_NAME))
    ).resolve()

    state = _load_sync_state(state_file)
    handled_run_ids = state["processed_run_ids"] | state["skipped_run_ids"]

    candidates = _collect_candidate_runs(
        repo=repo,
        workflows=workflows,
        lookback_runs=lookback_runs,
        timeout_seconds=timeout_seconds,
        handled_run_ids=handled_run_ids,
    )
    if not candidates:
        summary["reason"] = "no_new_runs"
        _save_sync_state(state_file, state)
        return summary

    rows_by_table = {name: 0 for name in SYNC_TABLES}
    processed_runs = 0
    synced_codes: Set[str] = set()

    with tempfile.TemporaryDirectory(prefix="dsa_cloud_sync_") as tmp_dir:
        temp_root = Path(tmp_dir)
        for run_id, workflow_name in candidates:
            run_dir = temp_root / str(run_id)
            run_dir.mkdir(parents=True, exist_ok=True)

            download_ok, download_msg = _download_artifact(
                repo=repo,
                run_id=run_id,
                artifact_name=artifact_name,
                destination_dir=run_dir,
                timeout_seconds=max(timeout_seconds, 60),
            )
            if not download_ok:
                logger.info("[CloudSync] run %s (%s) skipped: %s", run_id, workflow_name, download_msg)
                if download_msg in {"artifact_missing", "run_or_artifact_not_found"}:
                    state["skipped_run_ids"].add(run_id)
                continue

            remote_db_path = _find_downloaded_db_file(run_dir)
            if remote_db_path is None:
                logger.info("[CloudSync] run %s (%s) skipped: stock_analysis.db not found in artifact.", run_id, workflow_name)
                state["skipped_run_ids"].add(run_id)
                continue

            merged, merged_codes = _merge_remote_db_into_local(
                remote_db_path=remote_db_path,
                local_db_path=destination_db,
            )
            for table_name, count in merged.items():
                rows_by_table[table_name] = rows_by_table.get(table_name, 0) + int(count or 0)
            synced_codes.update(merged_codes)

            processed_runs += 1
            state["processed_run_ids"].add(run_id)
            state["skipped_run_ids"].discard(run_id)
            logger.info(
                "[CloudSync] merged run %s (%s): analysis_history=%s, news_intel=%s, fundamental_snapshot=%s, codes=%s",
                run_id,
                workflow_name,
                merged.get("analysis_history", 0),
                merged.get("news_intel", 0),
                merged.get("fundamental_snapshot", 0),
                _format_codes_for_log(merged_codes),
            )

    _save_sync_state(state_file, state)

    total_merged = sum(rows_by_table.values())
    summary.update(
        {
            "status": "ok",
            "reason": "",
            "processed_runs": processed_runs,
            "merged_rows": total_merged,
            "rows_by_table": rows_by_table,
            "synced_codes": sorted(synced_codes),
        }
    )
    return summary


def _collect_candidate_runs(
    repo: str,
    workflows: Sequence[str],
    lookback_runs: int,
    timeout_seconds: int,
    handled_run_ids: Set[int],
) -> List[Tuple[int, str]]:
    candidates: List[Tuple[int, str]] = []

    for workflow_name in workflows:
        run_list_ok, payload = _list_runs_for_workflow(
            repo=repo,
            workflow_name=workflow_name,
            limit=lookback_runs,
            timeout_seconds=timeout_seconds,
        )
        if not run_list_ok:
            continue

        for run in payload:
            run_id_raw = run.get("databaseId")
            try:
                run_id = int(run_id_raw)
            except Exception:
                continue

            if run_id in handled_run_ids:
                continue
            if run.get("status") != "completed":
                continue
            if run.get("conclusion") != "success":
                continue

            candidates.append((run_id, workflow_name))

    # Process older runs first to keep merged history in chronological order.
    candidates.sort(key=lambda x: x[0])
    return candidates


def _list_runs_for_workflow(
    repo: str,
    workflow_name: str,
    limit: int,
    timeout_seconds: int,
) -> Tuple[bool, List[Dict[str, Any]]]:
    ok, stdout, stderr = _run_gh(
        [
            "run",
            "list",
            "--repo",
            repo,
            "--workflow",
            workflow_name,
            "--limit",
            str(limit),
            "--json",
            "databaseId,status,conclusion,createdAt",
        ],
        timeout_seconds=timeout_seconds,
    )
    if not ok:
        logger.info("[CloudSync] list runs failed for %s: %s", workflow_name, stderr.strip() or "unknown error")
        return False, []

    try:
        payload = json.loads(stdout or "[]")
    except json.JSONDecodeError:
        logger.info("[CloudSync] invalid gh JSON for %s", workflow_name)
        return False, []
    if not isinstance(payload, list):
        return False, []
    return True, payload


def _download_artifact(
    repo: str,
    run_id: int,
    artifact_name: str,
    destination_dir: Path,
    timeout_seconds: int,
) -> Tuple[bool, str]:
    ok, _stdout, stderr = _run_gh(
        [
            "run",
            "download",
            str(run_id),
            "--repo",
            repo,
            "--name",
            artifact_name,
            "--dir",
            str(destination_dir),
        ],
        timeout_seconds=timeout_seconds,
    )
    if ok:
        return True, ""

    message = (stderr or "").strip()
    lowered = message.lower()
    if "no artifacts found" in lowered or "no artifact matches" in lowered:
        return False, "artifact_missing"
    if "authentication" in lowered or "not logged into" in lowered:
        return False, "gh_auth_required"
    if "http 404" in lowered:
        return False, "run_or_artifact_not_found"
    return False, message or "download_failed"


def _merge_remote_db_into_local(remote_db_path: Path, local_db_path: Path) -> Tuple[Dict[str, int], Set[str]]:
    rows_by_table = {name: 0 for name in SYNC_TABLES}
    merged_codes: Set[str] = set()
    if not remote_db_path.exists():
        return rows_by_table, merged_codes

    if not local_db_path.exists():
        shutil.copy2(remote_db_path, local_db_path)
        return _count_rows(local_db_path, SYNC_TABLES), _collect_codes_from_db(local_db_path, SYNC_TABLES)

    connection = sqlite3.connect(str(local_db_path))
    try:
        connection.execute("PRAGMA busy_timeout = 5000")
        connection.execute("ATTACH DATABASE ? AS remote", (str(remote_db_path),))
        for table_name in SYNC_TABLES:
            merged_count, codes = _merge_table(connection, table_name)
            rows_by_table[table_name] = merged_count
            merged_codes.update(codes)
        connection.commit()
        try:
            connection.execute("DETACH DATABASE remote")
        except sqlite3.OperationalError:
            # Ignore detach failures caused by previous statement interruption.
            pass
    finally:
        connection.close()
    return rows_by_table, merged_codes


def _merge_table(connection: sqlite3.Connection, table_name: str) -> Tuple[int, Set[str]]:
    if not _table_exists(connection, "remote", table_name):
        return 0, set()
    _ensure_local_table(connection, table_name)
    if not _table_exists(connection, "main", table_name):
        return 0, set()

    main_columns = _table_columns(connection, "main", table_name)
    remote_columns = _table_columns(connection, "remote", table_name)
    shared_columns = [col for col in remote_columns if col in main_columns and col != "id"]
    if not shared_columns:
        return 0, set()
    merged_codes = _candidate_codes_to_insert(connection, table_name, main_columns, remote_columns)

    quoted_columns = ", ".join(_quote_identifier(col) for col in shared_columns)

    if table_name == "analysis_history":
        delete_sql = f"""
            DELETE FROM main.{_quote_identifier(table_name)} AS l
            WHERE EXISTS (
                SELECT 1
                FROM remote.{_quote_identifier(table_name)} AS r
                WHERE COALESCE(l."query_id", '') = COALESCE(r."query_id", '')
                  AND COALESCE(l."code", '') = COALESCE(r."code", '')
                  AND COALESCE(r."created_at", '') > COALESCE(l."created_at", '')
            )
        """
        connection.execute(delete_sql)
        sql = f"""
            INSERT INTO main.{_quote_identifier(table_name)} ({quoted_columns})
            SELECT {quoted_columns}
            FROM remote.{_quote_identifier(table_name)} AS r
            WHERE NOT EXISTS (
                SELECT 1
                FROM main.{_quote_identifier(table_name)} AS l
                WHERE COALESCE(l."query_id", '') = COALESCE(r."query_id", '')
                  AND COALESCE(l."code", '') = COALESCE(r."code", '')
            )
        """
    elif table_name == "fundamental_snapshot":
        delete_sql = f"""
            DELETE FROM main.{_quote_identifier(table_name)} AS l
            WHERE EXISTS (
                SELECT 1
                FROM remote.{_quote_identifier(table_name)} AS r
                WHERE COALESCE(l."query_id", '') = COALESCE(r."query_id", '')
                  AND COALESCE(l."code", '') = COALESCE(r."code", '')
                  AND COALESCE(r."created_at", '') > COALESCE(l."created_at", '')
            )
        """
        connection.execute(delete_sql)
        sql = f"""
            INSERT INTO main.{_quote_identifier(table_name)} ({quoted_columns})
            SELECT {quoted_columns}
            FROM remote.{_quote_identifier(table_name)} AS r
            WHERE NOT EXISTS (
                SELECT 1
                FROM main.{_quote_identifier(table_name)} AS l
                WHERE COALESCE(l."query_id", '') = COALESCE(r."query_id", '')
                  AND COALESCE(l."code", '') = COALESCE(r."code", '')
            )
        """
    else:
        sql = f"""
            INSERT OR IGNORE INTO main.{_quote_identifier(table_name)} ({quoted_columns})
            SELECT {quoted_columns}
            FROM remote.{_quote_identifier(table_name)}
        """

    connection.execute(sql)
    return int(connection.execute("SELECT changes()").fetchone()[0] or 0), merged_codes


def _candidate_codes_to_insert(
    connection: sqlite3.Connection,
    table_name: str,
    main_columns: Sequence[str],
    remote_columns: Sequence[str],
) -> Set[str]:
    if "code" not in main_columns or "code" not in remote_columns:
        return set()

    if table_name == "analysis_history":
        sql = f"""
            SELECT DISTINCT r.code
            FROM remote.{_quote_identifier(table_name)} AS r
            LEFT JOIN main.{_quote_identifier(table_name)} AS l
              ON COALESCE(l."query_id", '') = COALESCE(r."query_id", '')
             AND COALESCE(l."code", '') = COALESCE(r."code", '')
            WHERE r.code IS NOT NULL AND TRIM(r.code) != ''
              AND (
                    l.id IS NULL
                 OR COALESCE(r."created_at", '') > COALESCE(l."created_at", '')
              )
        """
    elif table_name == "fundamental_snapshot":
        sql = f"""
            SELECT DISTINCT r.code
            FROM remote.{_quote_identifier(table_name)} AS r
            LEFT JOIN main.{_quote_identifier(table_name)} AS l
              ON COALESCE(l."query_id", '') = COALESCE(r."query_id", '')
             AND COALESCE(l."code", '') = COALESCE(r."code", '')
            WHERE r.code IS NOT NULL AND TRIM(r.code) != ''
              AND (
                    l.id IS NULL
                 OR COALESCE(r."created_at", '') > COALESCE(l."created_at", '')
              )
        """
    else:
        if "url" not in main_columns or "url" not in remote_columns:
            return set()
        sql = f"""
            SELECT DISTINCT r.code
            FROM remote.{_quote_identifier(table_name)} AS r
            LEFT JOIN main.{_quote_identifier(table_name)} AS l
              ON COALESCE(l."url", '') = COALESCE(r."url", '')
            WHERE r.code IS NOT NULL AND TRIM(r.code) != ''
              AND l.id IS NULL
        """

    return {
        str(row[0]).strip()
        for row in connection.execute(sql).fetchall()
        if row and row[0] and str(row[0]).strip()
    }


def _ensure_local_table(connection: sqlite3.Connection, table_name: str) -> None:
    if _table_exists(connection, "main", table_name):
        return

    row = connection.execute(
        "SELECT sql FROM remote.sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    if not row or not row[0]:
        return

    connection.execute(row[0])

    index_rows = connection.execute(
        "SELECT sql FROM remote.sqlite_master WHERE type='index' AND tbl_name=? AND sql IS NOT NULL",
        (table_name,),
    ).fetchall()
    for (index_sql,) in index_rows:
        try:
            connection.execute(index_sql)
        except sqlite3.OperationalError:
            continue


def _table_exists(connection: sqlite3.Connection, schema: str, table_name: str) -> bool:
    row = connection.execute(
        f"SELECT 1 FROM {schema}.sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def _table_columns(connection: sqlite3.Connection, schema: str, table_name: str) -> List[str]:
    rows = connection.execute(
        f"PRAGMA {schema}.table_info({_quote_sql_literal(table_name)})"
    ).fetchall()
    return [str(row[1]) for row in rows]


def _quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _quote_sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _count_rows(db_path: Path, table_names: Iterable[str]) -> Dict[str, int]:
    counts = {name: 0 for name in table_names}
    connection = sqlite3.connect(str(db_path))
    try:
        for name in table_names:
            try:
                row = connection.execute(
                    f"SELECT COUNT(1) FROM {_quote_identifier(name)}"
                ).fetchone()
                counts[name] = int(row[0] or 0) if row else 0
            except sqlite3.OperationalError:
                counts[name] = 0
    finally:
        connection.close()
    return counts


def _collect_codes_from_db(db_path: Path, table_names: Iterable[str]) -> Set[str]:
    codes: Set[str] = set()
    connection = sqlite3.connect(str(db_path))
    try:
        for table_name in table_names:
            if not _table_exists(connection, "main", table_name):
                continue
            columns = _table_columns(connection, "main", table_name)
            if "code" not in columns:
                continue
            rows = connection.execute(
                f"SELECT DISTINCT code FROM {_quote_identifier(table_name)} WHERE code IS NOT NULL AND TRIM(code) != ''"
            ).fetchall()
            for row in rows:
                if row and row[0]:
                    normalized = str(row[0]).strip()
                    if normalized:
                        codes.add(normalized)
    finally:
        connection.close()
    return codes


def _format_codes_for_log(codes: Iterable[str], limit: int = 20) -> str:
    normalized = sorted({str(code).strip() for code in codes if str(code).strip()})
    if not normalized:
        return "-"
    if len(normalized) <= limit:
        return ",".join(normalized)
    return f"{','.join(normalized[:limit])} ... (+{len(normalized) - limit} more)"


def _find_downloaded_db_file(directory: Path) -> Optional[Path]:
    matches = sorted(directory.rglob("stock_analysis.db"))
    if not matches:
        return None
    return matches[0]


def _run_gh(args: Sequence[str], timeout_seconds: int) -> Tuple[bool, str, str]:
    env = dict(os.environ)
    if not env.get("GH_TOKEN") and env.get("GITHUB_TOKEN"):
        env["GH_TOKEN"] = env["GITHUB_TOKEN"]

    command = ["gh", *args]
    try:
        completed = subprocess.run(
            command,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_seconds,
            env=env,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return False, "", f"timeout({timeout_seconds}s): {' '.join(command)}"
    except Exception as exc:
        return False, "", str(exc)

    return completed.returncode == 0, completed.stdout or "", completed.stderr or ""


def _resolve_repo_name() -> Optional[str]:
    explicit_repo = (os.getenv("CLOUD_HISTORY_SYNC_REPO", "") or "").strip()
    if explicit_repo:
        return explicit_repo

    env_repo = (os.getenv("GITHUB_REPOSITORY", "") or "").strip()
    if env_repo:
        return env_repo

    remote_url = _git_remote_origin_url()
    if not remote_url:
        return None

    # Match:
    # - https://github.com/owner/repo.git
    # - git@github.com:owner/repo.git
    # - ssh://git@github.com/owner/repo
    match = re.search(r"github\.com[:/](?P<repo>[^/\s]+/[^/\s]+?)(?:\.git)?$", remote_url)
    if not match:
        return None
    return match.group("repo")


def _git_remote_origin_url() -> str:
    try:
        completed = subprocess.run(
            ["git", "config", "--get", "remote.origin.url"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=5,
            check=False,
        )
    except Exception:
        return ""
    if completed.returncode != 0:
        return ""
    return (completed.stdout or "").strip()


def _parse_workflows(raw: str) -> List[str]:
    return [item.strip() for item in (raw or "").split(",") if item.strip()]


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    value = os.getenv(name)
    if value is None or not str(value).strip():
        return default
    try:
        parsed = int(str(value).strip())
    except ValueError:
        return default
    return max(minimum, min(maximum, parsed))


def _load_sync_state(path: Path) -> Dict[str, Set[int]]:
    state: Dict[str, Set[int]] = {
        "processed_run_ids": set(),
        "skipped_run_ids": set(),
    }
    if not path.exists():
        return state

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return state

    for key in ("processed_run_ids", "skipped_run_ids"):
        values = payload.get(key, [])
        if not isinstance(values, list):
            continue
        parsed: Set[int] = set()
        for item in values:
            try:
                parsed.add(int(item))
            except Exception:
                continue
        state[key] = parsed
    return state


def _save_sync_state(path: Path, state: Dict[str, Set[int]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    processed_ids = sorted(set(state.get("processed_run_ids", set())))
    skipped_ids = sorted(set(state.get("skipped_run_ids", set())))

    payload = {
        "processed_run_ids": processed_ids[-STATE_MAX_RUN_IDS:],
        "skipped_run_ids": skipped_ids[-STATE_MAX_RUN_IDS:],
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
