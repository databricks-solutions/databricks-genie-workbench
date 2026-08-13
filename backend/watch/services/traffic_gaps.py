"""Pure analysis for owner-reviewable benchmark candidate gaps.

Raw question text and user identifiers are inputs only.  Neither is included
in the result model, logged, or persisted by this module.
"""

from __future__ import annotations

import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Iterable

from backend.watch.models import TrafficGapAnalysis, TrafficGapCandidate

_ISO_DATE_RE = re.compile(r"\b\d{4}-\d{1,2}-\d{1,2}\b")
_NUMBER_RE = re.compile(r"(?<!\w)[+-]?(?:\d[\d,]*)(?:\.\d+)?(?!\w)")
_TRAILING_SENTENCE_PUNCTUATION_RE = re.compile(r"[?!.]+$")
_SPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class TrafficMessage:
    content: str
    conversation_id: str
    user_key: str
    status: str = ""
    feedback: str | None = None
    created_at: datetime | None = None


@dataclass
class _Family:
    users: set[str]
    conversations: list[str]
    conversation_users: dict[str, str]
    failed_conversations: list[str]
    negative_feedback_conversations: list[str]
    occurrence_count: int = 0
    failed_count: int = 0
    negative_feedback_count: int = 0
    first_seen_at: datetime | None = None
    last_seen_at: datetime | None = None


def normalize_question(value: str) -> str:
    """Return a conservative template key used for exact family matching.

    Only ISO dates and numeric literals are abstracted. Quoted business terms
    and meaningful punctuation are preserved; this is intentionally not
    semantic or fuzzy matching.
    """
    text = unicodedata.normalize("NFKC", str(value or "")).casefold()
    text = _ISO_DATE_RE.sub(" dateliteral ", text)
    text = _NUMBER_RE.sub(" numberliteral ", text)
    text = _TRAILING_SENTENCE_PUNCTUATION_RE.sub("", text)
    return _SPACE_RE.sub(" ", text).strip()


def _is_negative_feedback(value: str | None) -> bool:
    normalized = str(value or "").strip().upper()
    return normalized in {"NEGATIVE", "THUMBS_DOWN", "DISLIKE"}


def _update_seen(family: _Family, created_at: datetime | None) -> None:
    if created_at is None:
        return
    if family.first_seen_at is None or created_at < family.first_seen_at:
        family.first_seen_at = created_at
    if family.last_seen_at is None or created_at > family.last_seen_at:
        family.last_seen_at = created_at


def _evidence_conversations(family: _Family, signals: list[str]) -> list[str]:
    selected: list[str] = []

    def add(conversation_id: str) -> None:
        if conversation_id and conversation_id not in selected and len(selected) < 3:
            selected.append(conversation_id)

    for conversation_id in family.negative_feedback_conversations:
        add(conversation_id)
    for conversation_id in family.failed_conversations:
        add(conversation_id)

    if "cross_user_repeat" in signals:
        represented_users = {
            family.conversation_users.get(conversation_id, "")
            for conversation_id in selected
        }
        for conversation_id in family.conversations:
            user_key = family.conversation_users.get(conversation_id, "")
            if user_key and user_key not in represented_users:
                add(conversation_id)
                represented_users.add(user_key)
            if len(represented_users) >= 2 or len(selected) >= 3:
                break

    for conversation_id in family.conversations:
        add(conversation_id)
    return selected


def analyze_traffic_gaps(
    *,
    messages: Iterable[TrafficMessage],
    benchmark_questions: Iterable[str],
    conversation_url: Callable[[str], str],
) -> TrafficGapAnalysis:
    """Group traffic and return only uncovered, owner-actionable candidates."""
    families: dict[str, _Family] = defaultdict(
        lambda: _Family(
            users=set(),
            conversations=[],
            conversation_users={},
            failed_conversations=[],
            negative_feedback_conversations=[],
        )
    )
    scanned_message_count = 0
    for message in messages:
        status = str(message.status or "").strip().upper()
        if status not in {"COMPLETED", "FAILED"}:
            continue
        key = normalize_question(message.content)
        if not key:
            continue
        scanned_message_count += 1
        family = families[key]
        family.occurrence_count += 1
        if message.user_key:
            family.users.add(message.user_key)
        if message.conversation_id and message.conversation_id not in family.conversations:
            family.conversations.append(message.conversation_id)
        if message.conversation_id and message.user_key:
            family.conversation_users.setdefault(
                message.conversation_id, message.user_key
            )
        if status == "FAILED":
            family.failed_count += 1
            if message.conversation_id not in family.failed_conversations:
                family.failed_conversations.append(message.conversation_id)
        if _is_negative_feedback(message.feedback):
            family.negative_feedback_count += 1
            if message.conversation_id not in family.negative_feedback_conversations:
                family.negative_feedback_conversations.append(message.conversation_id)
        _update_seen(family, message.created_at)

    covered_keys = {
        key
        for question in benchmark_questions
        if (key := normalize_question(question))
    }
    covered_family_count = sum(key in covered_keys for key in families)

    ranked: list[tuple[str, _Family, list[str]]] = []
    for key, family in families.items():
        if key in covered_keys:
            continue
        signals: list[str] = []
        if family.negative_feedback_count:
            signals.append("negative_feedback")
        if family.failed_count:
            signals.append("failed")
        if len(family.users) >= 2 and family.occurrence_count >= 2:
            signals.append("cross_user_repeat")
        if signals:
            ranked.append((key, family, signals))

    ranked.sort(
        key=lambda item: (
            -item[1].negative_feedback_count,
            -item[1].failed_count,
            -len(item[1].users),
            -item[1].occurrence_count,
            item[0],
        )
    )

    candidates = [
        TrafficGapCandidate(
            candidate_id=f"candidate-{index}",
            occurrence_count=family.occurrence_count,
            distinct_user_count=len(family.users),
            failed_count=family.failed_count,
            negative_feedback_count=family.negative_feedback_count,
            signals=signals,
            conversation_urls=[
                conversation_url(conversation_id)
                for conversation_id in _evidence_conversations(family, signals)
            ],
            first_seen_at=family.first_seen_at,
            last_seen_at=family.last_seen_at,
        )
        for index, (_key, family, signals) in enumerate(ranked, start=1)
    ]
    return TrafficGapAnalysis(
        scanned_message_count=scanned_message_count,
        family_count=len(families),
        covered_family_count=covered_family_count,
        candidates=candidates,
    )
