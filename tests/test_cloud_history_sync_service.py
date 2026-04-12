# -*- coding: utf-8 -*-
"""Tests for cloud history sync merge dedupe by normalized stock code."""

import sqlite3
import tempfile
import unittest
from pathlib import Path

from src.services.cloud_history_sync_service import (
    _dedupe_local_analysis_history_by_code,
    _merge_remote_db_into_local,
)


ANALYSIS_HISTORY_SCHEMA = """
CREATE TABLE analysis_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query_id TEXT,
    code TEXT NOT NULL,
    name TEXT,
    report_type TEXT,
    sentiment_score INTEGER,
    operation_advice TEXT,
    trend_prediction TEXT,
    analysis_summary TEXT,
    raw_result TEXT,
    news_content TEXT,
    context_snapshot TEXT,
    ideal_buy REAL,
    secondary_buy REAL,
    stop_loss REAL,
    take_profit REAL,
    created_at TEXT
)
"""


class CloudHistorySyncServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.local_db = Path(self._tmp.name) / "local.db"
        self.remote_db = Path(self._tmp.name) / "remote.db"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _init_db(self, db_path: Path) -> None:
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(ANALYSIS_HISTORY_SCHEMA)
            conn.commit()

    def _insert_history(self, db_path: Path, *, query_id: str, code: str, created_at: str, name: str = "") -> None:
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute(
                """
                INSERT INTO analysis_history (
                    query_id, code, name, report_type, sentiment_score,
                    operation_advice, trend_prediction, analysis_summary,
                    raw_result, news_content, context_snapshot,
                    ideal_buy, secondary_buy, stop_loss, take_profit, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    query_id,
                    code,
                    name,
                    "daily",
                    70,
                    "hold",
                    "neutral",
                    "summary",
                    "{}",
                    "",
                    "",
                    None,
                    None,
                    None,
                    None,
                    created_at,
                ),
            )
            conn.commit()

    def _query_rows_by_code(self, db_path: Path, normalized_code: str):
        with sqlite3.connect(str(db_path)) as conn:
            return conn.execute(
                """
                SELECT query_id, code, created_at
                FROM analysis_history
                WHERE UPPER(TRIM(COALESCE(code, ''))) = ?
                ORDER BY created_at DESC, id DESC
                """,
                (normalized_code.upper(),),
            ).fetchall()

    def test_merge_analysis_history_keeps_latest_by_normalized_code(self) -> None:
        self._init_db(self.local_db)
        self._init_db(self.remote_db)

        self._insert_history(
            self.local_db,
            query_id="local_old",
            code="SNDK",
            created_at="2026-04-01T09:00:00",
            name="SanDisk",
        )

        self._insert_history(
            self.remote_db,
            query_id="remote_mid",
            code="sndk",
            created_at="2026-04-02T09:00:00",
            name="SanDisk",
        )
        self._insert_history(
            self.remote_db,
            query_id="remote_new",
            code="SNDK",
            created_at="2026-04-03T10:00:00",
            name="SanDisk",
        )

        _merge_remote_db_into_local(self.remote_db, self.local_db)

        rows = self._query_rows_by_code(self.local_db, "SNDK")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "remote_new")
        self.assertEqual(rows[0][1], "SNDK")
        self.assertEqual(rows[0][2], "2026-04-03T10:00:00")

    def test_merge_does_not_replace_when_remote_is_older(self) -> None:
        self._init_db(self.local_db)
        self._init_db(self.remote_db)

        self._insert_history(
            self.local_db,
            query_id="local_new",
            code="SNDK",
            created_at="2026-04-05T09:00:00",
        )
        self._insert_history(
            self.remote_db,
            query_id="remote_old",
            code="sndk",
            created_at="2026-04-04T09:00:00",
        )

        _merge_remote_db_into_local(self.remote_db, self.local_db)

        rows = self._query_rows_by_code(self.local_db, "SNDK")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "local_new")
        self.assertEqual(rows[0][2], "2026-04-05T09:00:00")

    def test_local_dedupe_keeps_latest_and_normalizes_code(self) -> None:
        self._init_db(self.local_db)

        self._insert_history(
            self.local_db,
            query_id="old",
            code="sndk",
            created_at="2026-04-01T09:00:00",
        )
        self._insert_history(
            self.local_db,
            query_id="new",
            code="SNDK ",
            created_at="2026-04-03T09:00:00",
        )

        removed = _dedupe_local_analysis_history_by_code(self.local_db)
        self.assertEqual(removed, 1)

        rows = self._query_rows_by_code(self.local_db, "SNDK")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "new")
        self.assertEqual(rows[0][1], "SNDK")
        self.assertEqual(rows[0][2], "2026-04-03T09:00:00")

    def test_first_sync_copy_also_dedupes_by_code(self) -> None:
        self._init_db(self.remote_db)

        self._insert_history(
            self.remote_db,
            query_id="remote_old",
            code="sndk",
            created_at="2026-04-01T09:00:00",
        )
        self._insert_history(
            self.remote_db,
            query_id="remote_new",
            code="SNDK",
            created_at="2026-04-04T09:00:00",
        )

        _merge_remote_db_into_local(self.remote_db, self.local_db)

        rows = self._query_rows_by_code(self.local_db, "SNDK")
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0][0], "remote_new")
        self.assertEqual(rows[0][1], "SNDK")


if __name__ == "__main__":
    unittest.main()
