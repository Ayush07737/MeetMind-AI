"""MeetMind AI — Identifier Recovery Closed Sprint Board Simulation (Q-0e Section D).

Simulates an engineering project's active sprint board consisting of 200 tickets
and 50 pull requests, seeded with all active identifiers from the synthetic
engineering corpus plus controlled confusable pairs for testing abstention margins.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class BoardItem:
    identifier: str  # e.g. "PLAT-101", "PR-482"
    item_type: str  # "ticket" or "pull_request"
    title: str
    spoken_aliases: tuple[str, ...]  # Canonical spoken representations


CORPUS_TRUE_IDENTIFIERS: list[tuple[str, str, str]] = [
    ("PLAT-101", "ticket", "Redis stream backlog trimming"),
    ("CORE-601", "ticket", "Async worker lifecycle recycle cap"),
    ("DATA-204", "ticket", "Neon PostgreSQL partition migration"),
    ("ENG-4521", "ticket", "CI timeout increase for integration tests"),
    ("ENG-789", "ticket", "Qdrant vector collection sharding"),
    ("FRONT-303", "ticket", "Chrome extension audio capture pipeline"),
    ("FRONT-304", "ticket", "Side panel latency telemetry visualizer"),
    ("FRONT-410", "ticket", "Prettier and ruff formatting hook in pre-commit"),
    ("INC-409", "ticket", "High P99 latency on Deepgram WebSocket upgrade"),
    ("INFRA-402", "ticket", "Terraform AWS Mumbai VM provisioning"),
    ("INFRA-909", "ticket", "Redis cluster replication and failover"),
    ("TECH-801", "ticket", "OpenTelemetry tracing across ingestion gateway"),
    ("ADR-008", "ticket", "Timing envelope preservation and budget check"),
    ("PR-482", "pull_request", "Fix worker recycle summary premature audit"),
    ("PR-490", "pull_request", "Kernel power modern standby classification"),
    ("PR-501", "pull_request", "Identifier recovery re-scorer package"),
    ("PR-515", "pull_request", "Multi-region architecture ADR-013"),
]

# Confusable pairs specifically tested in Section D
CONFUSABLE_TEST_PAIRS: list[tuple[str, str, str]] = [
    ("PLAT-123", "ticket", "Confusable test ticket A"),
    ("PLAT-213", "ticket", "Confusable test ticket B (transposition of 123)"),
    ("PR-428", "pull_request", "Confusable test PR (transposition of 482)"),
    ("CORE-610", "ticket", "Confusable test ticket (transposition of 601)"),
]

DIGIT_WORDS = {
    "0": "zero",
    "1": "one",
    "2": "two",
    "3": "three",
    "4": "four",
    "5": "five",
    "6": "six",
    "7": "seven",
    "8": "eight",
    "9": "nine",
}


def _make_spoken_aliases(identifier: str) -> tuple[str, ...]:
    """Generate spoken forms for an identifier (e.g. PLAT-101 -> ['plat one zero one', ...])."""
    prefix, _, num_str = identifier.partition("-")
    pfx_lower = prefix.lower()

    # Spoken digit by digit
    digit_words = " ".join(DIGIT_WORDS.get(c, c) for c in num_str)
    aliases = [
        f"{pfx_lower} {digit_words}",
        f"{pfx_lower} {num_str}",
        f"{pfx_lower}-{num_str}",
        f"{pfx_lower}{num_str}",
    ]
    # Spoken with 'to' for 'two' or 'for' for 'four' common ASR slips
    slip = digit_words.replace("two", "to").replace("four", "for")
    if slip != digit_words:
        aliases.append(f"{pfx_lower} {slip}")

    return tuple(dict.fromkeys(aliases))


def build_simulated_sprint_board() -> list[BoardItem]:
    """Construct closed sprint board of exactly 200 tickets and 50 PRs."""
    items: list[BoardItem] = []
    seen_ids: set[str] = set()

    # 1. Add corpus ground truth items
    for item_id, itype, title in CORPUS_TRUE_IDENTIFIERS:
        items.append(
            BoardItem(
                identifier=item_id,
                item_type=itype,
                title=title,
                spoken_aliases=_make_spoken_aliases(item_id),
            )
        )
        seen_ids.add(item_id)

    # 2. Add confusable test pairs
    for item_id, itype, title in CONFUSABLE_TEST_PAIRS:
        if item_id not in seen_ids:
            items.append(
                BoardItem(
                    identifier=item_id,
                    item_type=itype,
                    title=title,
                    spoken_aliases=_make_spoken_aliases(item_id),
                )
            )
            seen_ids.add(item_id)

    # 3. Fill up to 200 tickets using standard prefixes
    ticket_prefixes = ["PLAT", "CORE", "FRONT", "DATA", "ENG", "INFRA", "INC", "TECH", "SEC"]
    t_idx = 100
    p_idx = 0
    while sum(1 for it in items if it.item_type == "ticket") < 200:
        pfx = ticket_prefixes[p_idx % len(ticket_prefixes)]
        t_id = f"{pfx}-{t_idx}"
        if t_id not in seen_ids:
            items.append(
                BoardItem(
                    identifier=t_id,
                    item_type="ticket",
                    title=f"Synthetic sprint task {t_id}",
                    spoken_aliases=_make_spoken_aliases(t_id),
                )
            )
            seen_ids.add(t_id)
        p_idx += 1
        if p_idx % len(ticket_prefixes) == 0:
            t_idx += 1

    # 4. Fill up to 50 PRs
    pr_num = 400
    while sum(1 for it in items if it.item_type == "pull_request") < 50:
        pr_id = f"PR-{pr_num}"
        if pr_id not in seen_ids:
            items.append(
                BoardItem(
                    identifier=pr_id,
                    item_type="pull_request",
                    title=f"Synthetic pull request {pr_id}",
                    spoken_aliases=_make_spoken_aliases(pr_id),
                )
            )
            seen_ids.add(pr_id)
        pr_num += 1

    return items
