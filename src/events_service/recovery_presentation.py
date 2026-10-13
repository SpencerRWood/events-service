"""Telegram HTML rendering and lossless partitioning of recovery reports."""

from html import escape
from typing import Any
from urllib.parse import urlsplit

LIMIT = 4096


def units(text: str) -> int:
    """Telegram entity offsets use UTF-16; count conservatively in those units."""
    return len(text.encode("utf-16-le")) // 2


def safe_link(value: object) -> str:
    if not isinstance(value, str):
        raise ValueError("Recovery evidence link required")
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("Unsafe recovery evidence link")
    return value


def split_text(text: str, capacity: int) -> list[str]:
    """Only an individually oversized finding splits, without losing characters."""
    chunks: list[str] = []
    start = 0
    used = 0
    for index, char in enumerate(text):
        size = units(char)
        if used + size > capacity:
            chunks.append(text[start:index])
            start, used = index, 0
        used += size
    chunks.append(text[start:])
    return chunks


def telegram_parts(data: dict[str, Any]) -> list[dict[str, str]]:
    title = (
        "TEST / BACKUP & RECOVERY"
        if data.get("synthetic")
        else "WOOD / BACKUP & RECOVERY"
    )
    link = safe_link(data.get("evidence_link"))
    blocks = data.get("report_blocks")
    if not isinstance(blocks, list) or not blocks:
        raise ValueError("Recovery report blocks required")
    entries: list[tuple[str, str]] = []
    for block in blocks:
        if (
            not isinstance(block, dict)
            or set(block) != {"heading", "text"}
            or not all(isinstance(v, str) for v in block.values())
        ):
            raise ValueError("Invalid recovery report block")
        entries.append((block["heading"], block["text"]))
    entries.append(("Evidence", "View Dagster results: " + link))

    def displayed(group: list[tuple[str, str]]) -> list[tuple[str, str]]:
        result = []
        previous = ""
        for heading, text in group:
            result.append((heading if heading != previous else "", text))
            previous = heading
        return result

    def pack(count: int) -> list[list[tuple[str, str]]]:
        groups: list[list[tuple[str, str]]] = [[]]
        for heading, text in entries:
            remaining = text
            while True:
                index = len(groups)
                header = title if count == 1 else f"{title} — {index} of {count}"
                used = units(header) + sum(
                    units((h + "\n" if h else "") + t) + 2
                    for h, t in displayed(groups[-1])
                )
                visible_heading = (
                    heading if not groups[-1] or heading != groups[-1][-1][0] else ""
                )
                block_size = (
                    units(
                        (visible_heading + "\n" if visible_heading else "") + remaining
                    )
                    + 2
                )
                if used + block_size <= LIMIT:
                    groups[-1].append((heading, remaining))
                    break
                if groups[-1]:
                    groups.append([])
                    continue
                capacity = LIMIT - used - units(heading) - 3
                if capacity < 1:
                    raise ValueError("Recovery heading exceeds Telegram limit")
                chunks = split_text(remaining, capacity)
                groups[-1].append((heading, chunks[0]))
                remaining = "".join(chunks[1:])
                groups.append([])
        return groups

    count = 1
    while True:
        groups = pack(count)
        if len(groups) == count:
            break
        count = len(groups)
    parts = []
    for index, original_group in enumerate(groups, 1):
        group = displayed(original_group)
        header = title if count == 1 else f"{title} — {index} of {count}"
        plain = (
            header + "\n\n" + "\n\n".join((h + "\n" if h else "") + t for h, t in group)
        )
        rich = (
            "<b>"
            + escape(header)
            + "</b>\n\n"
            + "\n\n".join(
                ("<b>" + escape(h) + "</b>\n" if h else "")
                + (
                    '<a href="'
                    + escape(link, quote=True)
                    + '">View Dagster results</a>'
                    if h == "Evidence" and t == "View Dagster results: " + link
                    else escape(t)
                )
                for h, t in group
            )
        )
        parts.append({"plain": plain, "html": rich})
    return parts
