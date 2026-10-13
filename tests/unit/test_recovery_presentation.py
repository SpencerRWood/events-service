"""Lossless partitioning and Telegram entity safety."""

import re
from html import unescape

import pytest

from events_service.recovery_presentation import telegram_parts, units


def report(text: str) -> dict[str, object]:
    return {
        "evidence_link": "https://dagster.example.test/runs/current",
        "report_blocks": [{"heading": "Recovery dependencies", "text": text}],
    }


def test_html_escapes_and_preserves_diagnostics_as_text() -> None:
    text = 'Homelab: <script> & "credentials" 💾'
    (part,) = telegram_parts(report(text))
    assert "<script>" not in part["html"]
    assert text in part["plain"]
    assert text in unescape(re.sub("<[^>]+>", "", part["html"]))
    assert part["html"].count("<a ") == 1
    assert "runs/current" in part["html"]


@pytest.mark.parametrize("text", ["x" * 4096, "💾" * 5000, "<>&" * 4000])
def test_oversized_individual_finding_preserved(text: str) -> None:
    parts = telegram_parts(report(text))
    assert len(parts) > 1
    assert all(units(p["plain"]) <= 4096 for p in parts)
    reconstructed = "".join(
        p["plain"].split("Recovery dependencies\n", 1)[1].split("\n\nEvidence", 1)[0]
        for p in parts
        if "Recovery dependencies\n" in p["plain"]
    )
    assert reconstructed == text
    assert sum("View Dagster results" in p["plain"] for p in parts) == 1
    assert all(f"{i} of {len(parts)}" in p["plain"] for i, p in enumerate(parts, 1))


def test_all_findings_fit_or_split_without_count_limit() -> None:
    data = report("first")
    data["report_blocks"] = [
        {"heading": "Recovery verification", "text": f"Finding {i}: " + "action " * 25}
        for i in range(100)
    ]
    parts = telegram_parts(data)
    for i in range(100):
        assert sum(f"Finding {i}:" in p["plain"] for p in parts) == 1
    assert all(units(p["plain"]) <= 4096 for p in parts)
    assert len(telegram_parts(report("Short report"))) == 1


@pytest.mark.parametrize(
    "link",
    [
        "javascript:alert(1)",
        "https://user:pass@example.test",
        "https://example.test?token=secret",
        "http://example.test",
    ],
)
def test_unsafe_links_rejected(link: str) -> None:
    with pytest.raises(ValueError, match="Unsafe"):
        telegram_parts(report("text") | {"evidence_link": link})


def test_synthetic_title_is_not_production() -> None:
    (part,) = telegram_parts(report("Test data") | {"synthetic": True})
    assert part["plain"].startswith("TEST / BACKUP & RECOVERY")
