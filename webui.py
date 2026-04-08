# -*- coding: utf-8 -*-
"""
===================================
WebUI 启动脚本
===================================

用于启动 Web 服务界面。
直接运行 `python webui.py` 将启动 Web 后端服务。

等效命令：
    python main.py --webui-only

Usage:
  python webui.py
  WEBUI_HOST=0.0.0.0 WEBUI_PORT=8000 python webui.py
"""

from __future__ import annotations

import os
import logging

logger = logging.getLogger(__name__)


def _format_synced_codes_for_log(codes: list[str], limit: int = 30) -> str:
    normalized = sorted({str(code).strip() for code in codes if str(code).strip()})
    if not normalized:
        return "-"
    if len(normalized) <= limit:
        return ",".join(normalized)
    return f"{','.join(normalized[:limit])} ... (+{len(normalized) - limit} more)"


def main() -> int:
    """
    启动 Web 服务
    """
    # 兼容旧版环境变量名
    host = os.getenv("WEBUI_HOST", os.getenv("API_HOST", "127.0.0.1"))
    port = int(os.getenv("WEBUI_PORT", os.getenv("API_PORT", "8000")))

    print(f"正在启动 Web 服务: http://{host}:{port}")
    print(f"API 文档: http://{host}:{port}/docs")
    print()

    try:
        import uvicorn
        from src.config import get_config, setup_env
        from src.services.cloud_history_sync_service import sync_cloud_history_from_github_actions
        from src.logging_config import setup_logging

        setup_env()
        setup_logging(log_prefix="web_server")
        try:
            config = get_config()
            result = sync_cloud_history_from_github_actions(local_db_path=config.database_path)
            if result.get("status") != "ok":
                logger.info("[CloudSync] skipped: %s", result.get("reason", "unknown"))
            else:
                logger.info(
                    "[CloudSync] done: processed_runs=%s merged_rows=%s codes=%s",
                    result.get("processed_runs", 0),
                    result.get("merged_rows", 0),
                    _format_synced_codes_for_log(result.get("synced_codes", []) or []),
                )
        except Exception as sync_exc:
            logger.warning("[CloudSync] startup sync failed: %s", sync_exc)

        uvicorn.run(
            "api.app:app",
            host=host,
            port=port,
            log_level="info",
        )
    except KeyboardInterrupt:
        pass

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
