from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from shared.models import EventEnvelope


def test_event_envelope_requires_versioned_core_fields() -> None:
    envelope = EventEnvelope(
        event_id="evt-1",
        event_type="wood.test",
        source="tests",
        occurred_at=datetime(2026, 8, 12, tzinfo=UTC),
    )

    assert envelope.schema_version == "v1"
    assert envelope.severity == "info"
    assert envelope.payload == {}


def test_event_envelope_rejects_unversioned_extra_fields() -> None:
    with pytest.raises(ValidationError):
        EventEnvelope(
            event_id="evt-1",
            event_type="wood.test",
            source="tests",
            occurred_at=datetime(2026, 8, 12, tzinfo=UTC),
            unexpected=True,
        )
