import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from api.retention import RetentionConfig, retention_config_from_env, sweep_retention


class FakePool:
    def __init__(self, result: int = 3) -> None:
        self.result = result
        self.calls: list[tuple[str, tuple[object, ...]]] = []

    async def fetchval(self, query: str, *args: object) -> int:
        self.calls.append((query, args))
        return self.result


def test_retention_config_defaults_to_infinite_ttls(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("CENTAUR_RETENTION_ATTACHMENTS_TTL_DAYS", raising=False)
    monkeypatch.delenv("CENTAUR_RETENTION_TRANSCRIPTS_TTL_DAYS", raising=False)
    monkeypatch.delenv("CENTAUR_RETENTION_DRY_RUN", raising=False)

    config = retention_config_from_env()

    assert config.attachments_ttl_s is None
    assert config.transcripts_ttl_s is None
    assert not config.enabled
    assert not config.dry_run


def test_retention_config_parses_positive_ttls(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CENTAUR_RETENTION_ATTACHMENTS_TTL_DAYS", "7")
    monkeypatch.setenv("CENTAUR_RETENTION_TRANSCRIPTS_TTL_DAYS", "30")
    monkeypatch.setenv("CENTAUR_RETENTION_SWEEP_INTERVAL_SECONDS", "120")
    monkeypatch.setenv("CENTAUR_RETENTION_BATCH_SIZE", "25")
    monkeypatch.setenv("CENTAUR_RETENTION_DRY_RUN", "true")

    config = retention_config_from_env()

    assert config.attachments_ttl_s == 7 * 86400
    assert config.transcripts_ttl_s == 30 * 86400
    assert config.interval_s == 120
    assert config.batch_size == 25
    assert config.dry_run


@pytest.mark.asyncio
async def test_sweep_retention_is_noop_when_ttls_are_infinite() -> None:
    pool = FakePool()

    result = await sweep_retention(pool, RetentionConfig(None, None))

    assert result == {}
    assert pool.calls == []


@pytest.mark.asyncio
async def test_sweep_retention_dry_run_counts_candidates_without_mutation() -> None:
    pool = FakePool(result=2)
    config = RetentionConfig(
        attachments_ttl_s=86400,
        transcripts_ttl_s=None,
        batch_size=10,
        dry_run=True,
    )

    result = await sweep_retention(pool, config)

    assert result == {"attachments": 2}
    assert len(pool.calls) == 1
    query, args = pool.calls[0]
    assert "SELECT COUNT(*)::int" in query
    assert "DELETE FROM attachments" not in query
    assert args == (86400, 10)


@pytest.mark.asyncio
async def test_sweep_retention_redacts_transcript_targets() -> None:
    pool = FakePool(result=1)
    config = RetentionConfig(
        attachments_ttl_s=None,
        transcripts_ttl_s=30 * 86400,
        batch_size=5,
    )

    result = await sweep_retention(pool, config)

    assert result == {
        "chat_messages_parts": 1,
        "agent_message_requests_event_json": 1,
        "agent_execution_events_event_json": 1,
        "agent_execution_requests_result_text": 1,
        "agent_final_delivery_outbox_final_payload": 1,
    }
    assert len(pool.calls) == 5
    assert any("UPDATE chat_messages" in query for query, _ in pool.calls)
    assert any("UPDATE agent_message_requests" in query for query, _ in pool.calls)
    assert any("UPDATE agent_execution_events" in query for query, _ in pool.calls)
    assert any("UPDATE agent_execution_requests" in query for query, _ in pool.calls)
    assert any("UPDATE agent_final_delivery_outbox" in query for query, _ in pool.calls)


@pytest.mark.asyncio
async def test_final_delivery_retention_only_targets_terminal_deliveries() -> None:
    pool = FakePool(result=1)
    config = RetentionConfig(
        attachments_ttl_s=None,
        transcripts_ttl_s=30 * 86400,
        batch_size=5,
        dry_run=True,
    )

    await sweep_retention(pool, config)

    final_delivery_queries = [
        query for query, _ in pool.calls if "agent_final_delivery_outbox" in query
    ]
    assert len(final_delivery_queries) == 1
    assert "SELECT COUNT(*)::int" in final_delivery_queries[0]
    assert "state IN ('delivered', 'dead_letter')" in final_delivery_queries[0]
    assert "final_payload->>'type' IS DISTINCT FROM 'retention.redacted'" in final_delivery_queries[0]
