from __future__ import annotations

import asyncio
import os
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from typing import Any

import structlog

from api.vm_metrics import record_retention_sweep

log = structlog.get_logger()

_INFINITE_TTL_VALUES = {"", "0", "false", "inf", "infinite", "infinity", "none", "off", "disabled"}
_DEFAULT_INTERVAL_S = 3600
_DEFAULT_BATCH_SIZE = 500
_retention_task: asyncio.Task | None = None
_retention_stop: asyncio.Event | None = None


@dataclass(frozen=True)
class RetentionConfig:
    attachments_ttl_s: float | None
    transcripts_ttl_s: float | None
    interval_s: float = _DEFAULT_INTERVAL_S
    batch_size: int = _DEFAULT_BATCH_SIZE
    dry_run: bool = False

    @property
    def enabled(self) -> bool:
        return self.attachments_ttl_s is not None or self.transcripts_ttl_s is not None


@dataclass(frozen=True)
class RetentionTarget:
    name: str
    action: str
    ttl_s: float | None
    count: Callable[[Any, float, int], Awaitable[int]]
    apply: Callable[[Any, float, int], Awaitable[int]]


def _parse_ttl_days(value: str | None) -> float | None:
    raw = (value or "").strip().lower()
    if raw in _INFINITE_TTL_VALUES:
        return None
    days = float(raw)
    if days <= 0:
        return None
    return days * 86400


def _parse_positive_float(value: str | None, default: float) -> float:
    if value is None or not value.strip():
        return default
    parsed = float(value)
    return parsed if parsed > 0 else default


def _parse_positive_int(value: str | None, default: int) -> int:
    if value is None or not value.strip():
        return default
    parsed = int(value)
    return parsed if parsed > 0 else default


def _parse_bool(value: str | None, default: bool = False) -> bool:
    if value is None or not value.strip():
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def retention_config_from_env() -> RetentionConfig:
    return RetentionConfig(
        attachments_ttl_s=_parse_ttl_days(os.getenv("CENTAUR_RETENTION_ATTACHMENTS_TTL_DAYS")),
        transcripts_ttl_s=_parse_ttl_days(os.getenv("CENTAUR_RETENTION_TRANSCRIPTS_TTL_DAYS")),
        interval_s=_parse_positive_float(
            os.getenv("CENTAUR_RETENTION_SWEEP_INTERVAL_SECONDS"),
            _DEFAULT_INTERVAL_S,
        ),
        batch_size=_parse_positive_int(
            os.getenv("CENTAUR_RETENTION_BATCH_SIZE"),
            _DEFAULT_BATCH_SIZE,
        ),
        dry_run=_parse_bool(os.getenv("CENTAUR_RETENTION_DRY_RUN"), default=False),
    )


async def start_retention_sweeper(pool: Any, config: RetentionConfig | None = None) -> None:
    global _retention_stop, _retention_task

    config = config or retention_config_from_env()
    if not config.enabled:
        log.info(
            "retention_sweeper_disabled",
            attachments_ttl_s=config.attachments_ttl_s,
            transcripts_ttl_s=config.transcripts_ttl_s,
        )
        return
    if _retention_task is not None and not _retention_task.done():
        return

    _retention_stop = asyncio.Event()
    _retention_task = asyncio.create_task(_retention_loop(pool, config, _retention_stop))
    log.info(
        "retention_sweeper_started",
        attachments_ttl_s=config.attachments_ttl_s,
        transcripts_ttl_s=config.transcripts_ttl_s,
        interval_s=config.interval_s,
        batch_size=config.batch_size,
        dry_run=config.dry_run,
    )


async def stop_retention_sweeper() -> None:
    global _retention_stop, _retention_task

    task = _retention_task
    stop = _retention_stop
    _retention_task = None
    _retention_stop = None
    if task is None:
        return
    if stop is not None:
        stop.set()
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task
    log.info("retention_sweeper_stopped")


async def _retention_loop(pool: Any, config: RetentionConfig, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            await sweep_retention(pool, config)
        except Exception:
            log.warning("retention_sweep_failed", exc_info=True)
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=config.interval_s)
        except asyncio.TimeoutError:
            pass


async def sweep_retention(pool: Any, config: RetentionConfig | None = None) -> dict[str, int]:
    config = config or retention_config_from_env()
    if not config.enabled:
        return {}

    results: dict[str, int] = {}
    for target in _targets(config):
        if target.ttl_s is None:
            continue
        count = (
            await target.count(pool, target.ttl_s, config.batch_size)
            if config.dry_run
            else await target.apply(pool, target.ttl_s, config.batch_size)
        )
        results[target.name] = count
        record_retention_sweep(
            target=target.name,
            action="candidate" if config.dry_run else target.action,
            dry_run=config.dry_run,
            count=count,
        )
        log.info(
            "retention_sweep_target",
            target=target.name,
            ttl_s=target.ttl_s,
            batch_size=config.batch_size,
            dry_run=config.dry_run,
            count=count,
        )
    return results


def _targets(config: RetentionConfig) -> list[RetentionTarget]:
    return [
        RetentionTarget(
            "attachments",
            "deleted",
            config.attachments_ttl_s,
            _count_attachments,
            _delete_attachments,
        ),
        RetentionTarget(
            "chat_messages_parts",
            "redacted",
            config.transcripts_ttl_s,
            _count_chat_messages,
            _redact_chat_messages,
        ),
        RetentionTarget(
            "agent_message_requests_event_json",
            "redacted",
            config.transcripts_ttl_s,
            _count_agent_message_requests,
            _redact_agent_message_requests,
        ),
        RetentionTarget(
            "agent_execution_events_event_json",
            "redacted",
            config.transcripts_ttl_s,
            _count_agent_execution_events,
            _redact_agent_execution_events,
        ),
        RetentionTarget(
            "agent_execution_requests_result_text",
            "redacted",
            config.transcripts_ttl_s,
            _count_agent_execution_requests,
            _redact_agent_execution_requests,
        ),
        RetentionTarget(
            "agent_final_delivery_outbox_final_payload",
            "redacted",
            config.transcripts_ttl_s,
            _count_agent_final_delivery_outbox,
            _redact_agent_final_delivery_outbox,
        ),
    ]


async def _count_attachments(pool: Any, ttl_s: float, batch_size: int) -> int:
    return await pool.fetchval(
        """
        SELECT COUNT(*)::int
        FROM (
            SELECT id
            FROM attachments
            WHERE created_at < NOW() - make_interval(secs => $1::double precision)
            ORDER BY created_at
            LIMIT $2
        ) candidate
        """,
        ttl_s,
        batch_size,
    )


async def _delete_attachments(pool: Any, ttl_s: float, batch_size: int) -> int:
    return await pool.fetchval(
        """
        WITH doomed AS (
            SELECT id
            FROM attachments
            WHERE created_at < NOW() - make_interval(secs => $1::double precision)
            ORDER BY created_at
            LIMIT $2
        ),
        deleted AS (
            DELETE FROM attachments a
            USING doomed
            WHERE a.id = doomed.id
            RETURNING 1
        )
        SELECT COUNT(*)::int FROM deleted
        """,
        ttl_s,
        batch_size,
    )


async def _count_chat_messages(pool: Any, ttl_s: float, batch_size: int) -> int:
    return await pool.fetchval(
        """
        SELECT COUNT(*)::int
        FROM (
            SELECT id
            FROM chat_messages
            WHERE created_at < NOW() - make_interval(secs => $1::double precision)
              AND parts <> '[]'::jsonb
            ORDER BY created_at
            LIMIT $2
        ) candidate
        """,
        ttl_s,
        batch_size,
    )


async def _redact_chat_messages(pool: Any, ttl_s: float, batch_size: int) -> int:
    return await pool.fetchval(
        """
        WITH doomed AS (
            SELECT id
            FROM chat_messages
            WHERE created_at < NOW() - make_interval(secs => $1::double precision)
              AND parts <> '[]'::jsonb
            ORDER BY created_at
            LIMIT $2
        ),
        updated AS (
            UPDATE chat_messages cm
            SET parts = '[]'::jsonb,
                metadata = cm.metadata || jsonb_build_object(
                    'retention_redacted', true,
                    'retention_redacted_at', NOW()
                )
            FROM doomed
            WHERE cm.id = doomed.id
            RETURNING 1
        )
        SELECT COUNT(*)::int FROM updated
        """,
        ttl_s,
        batch_size,
    )


async def _count_agent_message_requests(pool: Any, ttl_s: float, batch_size: int) -> int:
    return await pool.fetchval(
        """
        SELECT COUNT(*)::int
        FROM (
            SELECT thread_key, message_id
            FROM agent_message_requests
            WHERE created_at < NOW() - make_interval(secs => $1::double precision)
              AND event_json->>'type' IS DISTINCT FROM 'retention.redacted'
            ORDER BY created_at
            LIMIT $2
        ) candidate
        """,
        ttl_s,
        batch_size,
    )


async def _redact_agent_message_requests(pool: Any, ttl_s: float, batch_size: int) -> int:
    return await pool.fetchval(
        """
        WITH doomed AS (
            SELECT thread_key, message_id
            FROM agent_message_requests
            WHERE created_at < NOW() - make_interval(secs => $1::double precision)
              AND event_json->>'type' IS DISTINCT FROM 'retention.redacted'
            ORDER BY created_at
            LIMIT $2
        ),
        updated AS (
            UPDATE agent_message_requests mr
            SET event_json = jsonb_build_object(
                    'type', 'retention.redacted',
                    'redacted_columns', jsonb_build_array('event_json'),
                    'redacted_at', NOW()
                ),
                metadata = mr.metadata || jsonb_build_object(
                    'retention_redacted', true,
                    'retention_redacted_at', NOW()
                )
            FROM doomed
            WHERE mr.thread_key = doomed.thread_key
              AND mr.message_id = doomed.message_id
            RETURNING 1
        )
        SELECT COUNT(*)::int FROM updated
        """,
        ttl_s,
        batch_size,
    )


async def _count_agent_execution_events(pool: Any, ttl_s: float, batch_size: int) -> int:
    return await pool.fetchval(
        """
        SELECT COUNT(*)::int
        FROM (
            SELECT event_id
            FROM agent_execution_events
            WHERE created_at < NOW() - make_interval(secs => $1::double precision)
              AND event_json->>'type' IS DISTINCT FROM 'retention.redacted'
            ORDER BY event_id
            LIMIT $2
        ) candidate
        """,
        ttl_s,
        batch_size,
    )


async def _redact_agent_execution_events(pool: Any, ttl_s: float, batch_size: int) -> int:
    return await pool.fetchval(
        """
        WITH doomed AS (
            SELECT event_id
            FROM agent_execution_events
            WHERE created_at < NOW() - make_interval(secs => $1::double precision)
              AND event_json->>'type' IS DISTINCT FROM 'retention.redacted'
            ORDER BY event_id
            LIMIT $2
        ),
        updated AS (
            UPDATE agent_execution_events ee
            SET event_json = jsonb_build_object(
                'type', 'retention.redacted',
                'redacted_columns', jsonb_build_array('event_json'),
                'redacted_at', NOW()
            )
            FROM doomed
            WHERE ee.event_id = doomed.event_id
            RETURNING 1
        )
        SELECT COUNT(*)::int FROM updated
        """,
        ttl_s,
        batch_size,
    )


async def _count_agent_execution_requests(pool: Any, ttl_s: float, batch_size: int) -> int:
    return await pool.fetchval(
        """
        SELECT COUNT(*)::int
        FROM (
            SELECT execution_id
            FROM agent_execution_requests
            WHERE COALESCE(completed_at, updated_at, created_at)
                < NOW() - make_interval(secs => $1::double precision)
              AND result_text IS NOT NULL
            ORDER BY COALESCE(completed_at, updated_at, created_at)
            LIMIT $2
        ) candidate
        """,
        ttl_s,
        batch_size,
    )


async def _redact_agent_execution_requests(pool: Any, ttl_s: float, batch_size: int) -> int:
    return await pool.fetchval(
        """
        WITH doomed AS (
            SELECT execution_id
            FROM agent_execution_requests
            WHERE COALESCE(completed_at, updated_at, created_at)
                < NOW() - make_interval(secs => $1::double precision)
              AND result_text IS NOT NULL
            ORDER BY COALESCE(completed_at, updated_at, created_at)
            LIMIT $2
        ),
        updated AS (
            UPDATE agent_execution_requests er
            SET result_text = NULL,
                metadata = er.metadata || jsonb_build_object(
                    'retention_redacted', true,
                    'retention_redacted_at', NOW(),
                    'retention_redacted_columns', jsonb_build_array('result_text')
                ),
                updated_at = NOW()
            FROM doomed
            WHERE er.execution_id = doomed.execution_id
            RETURNING 1
        )
        SELECT COUNT(*)::int FROM updated
        """,
        ttl_s,
        batch_size,
    )


async def _count_agent_final_delivery_outbox(pool: Any, ttl_s: float, batch_size: int) -> int:
    return await pool.fetchval(
        """
        SELECT COUNT(*)::int
        FROM (
            SELECT execution_id
            FROM agent_final_delivery_outbox
            WHERE COALESCE(delivered_at, updated_at, created_at)
                < NOW() - make_interval(secs => $1::double precision)
              AND state IN ('delivered', 'dead_letter')
              AND final_payload IS NOT NULL
              AND final_payload->>'type' IS DISTINCT FROM 'retention.redacted'
            ORDER BY COALESCE(delivered_at, updated_at, created_at)
            LIMIT $2
        ) candidate
        """,
        ttl_s,
        batch_size,
    )


async def _redact_agent_final_delivery_outbox(pool: Any, ttl_s: float, batch_size: int) -> int:
    return await pool.fetchval(
        """
        WITH doomed AS (
            SELECT execution_id
            FROM agent_final_delivery_outbox
            WHERE COALESCE(delivered_at, updated_at, created_at)
                < NOW() - make_interval(secs => $1::double precision)
              AND state IN ('delivered', 'dead_letter')
              AND final_payload IS NOT NULL
              AND final_payload->>'type' IS DISTINCT FROM 'retention.redacted'
            ORDER BY COALESCE(delivered_at, updated_at, created_at)
            LIMIT $2
        ),
        updated AS (
            UPDATE agent_final_delivery_outbox o
            SET final_payload = jsonb_build_object(
                    'type', 'retention.redacted',
                    'redacted_columns', jsonb_build_array('final_payload'),
                    'redacted_at', NOW()
                ),
                updated_at = NOW()
            FROM doomed
            WHERE o.execution_id = doomed.execution_id
            RETURNING 1
        )
        SELECT COUNT(*)::int FROM updated
        """,
        ttl_s,
        batch_size,
    )
