import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, TypeVar


class SceneSlot(Protocol):
    index: int
    start: float
    end: float
    text: str


TSlot = TypeVar("TSlot", bound=SceneSlot)


@dataclass
class SceneCue:
    narration: str
    start: float | None = None


def normalize_match_text(value: str) -> str:
    return re.sub(r"[\s\"'“”‘’.,!?…~·ㆍ:;()\[\]{}<>《》〈〉「」『』-]+", "", value).lower()


def parse_scene_time(value: str) -> float | None:
    raw = value.strip().strip('"')
    if not raw:
        return None
    start_text = re.split(r"\s*[~\-–—]\s*", raw, maxsplit=1)[0].strip()
    match = re.match(r"^(\d+):(\d{1,2})(?::(\d{1,2}))?(?:[,.](\d{1,3}))?$", start_text)
    if not match:
        return None
    first, second, third, millis = match.groups()
    if third is None:
        hours = 0
        minutes = int(first)
        seconds = int(second)
    else:
        hours = int(first)
        minutes = int(second)
        seconds = int(third)
    millis_value = int((millis or "0").ljust(3, "0")[:3])
    return hours * 3600 + minutes * 60 + seconds + millis_value / 1000


def has_meaningful_overlap(left: str, right: str, min_length: int = 4) -> bool:
    if not left or not right:
        return False
    if left in right or right in left:
        return True
    for size in range(min(len(left), len(right)), min_length - 1, -1):
        for start in range(0, len(left) - size + 1):
            if left[start : start + size] in right:
                return True
    return False


def text_ngrams(value: str, size: int = 2) -> set[str]:
    if len(value) < size:
        return {value} if value else set()
    return {value[index : index + size] for index in range(len(value) - size + 1)}


def text_similarity(left: str, right: str) -> float:
    left_grams = text_ngrams(left)
    right_grams = text_ngrams(right)
    if not left_grams or not right_grams:
        return 0.0
    overlap = len(left_grams & right_grams)
    return (2 * overlap) / (len(left_grams) + len(right_grams))


def read_scene_table(path: Path) -> list[SceneCue]:
    text = path.read_text(encoding="utf-8-sig", errors="replace")
    first_line = next((line for line in text.splitlines() if line.strip()), "")
    delimiter = "\t" if "\t" in first_line else ","
    reader = csv.DictReader(text.splitlines(), delimiter=delimiter)
    if not reader.fieldnames:
        raise ValueError("AI 장면표 헤더를 찾지 못했습니다.")

    narration_key = None
    time_key = None
    for key in reader.fieldnames:
        cleaned = (key or "").strip().replace(" ", "")
        if cleaned in {"한글내레이션", "내레이션", "대사", "시작대사", "시작자막"}:
            narration_key = key
        if cleaned in {"시작~끝", "시작끝", "시간", "구간", "길이"}:
            time_key = key
    if narration_key is None:
        raise ValueError("AI 장면표에는 '한글 내레이션' 또는 '시작 자막' 열이 있어야 합니다.")

    cues: list[SceneCue] = []
    for row in reader:
        narration = (row.get(narration_key) or "").strip().strip('"')
        if narration:
            start = parse_scene_time(row.get(time_key) or "") if time_key else None
            cues.append(SceneCue(narration=narration, start=start))
    if not cues:
        raise ValueError("AI 장면표에서 내레이션 문구를 찾지 못했습니다.")
    return cues


def find_slot_by_time(slots: list[TSlot], seconds: float, used_indices: set[int]) -> TSlot | None:
    candidates = [slot for slot in slots if slot.index not in used_indices]
    if not candidates:
        return None
    return min(candidates, key=lambda slot: abs(slot.start - seconds))


def find_slot_by_text(
    slots: list[TSlot],
    needle: str,
    used_indices: set[int],
    max_joined_slots: int,
    min_partial_length: int,
    min_similarity: float,
    time_hint: float | None = None,
    window_before: float = 12.0,
    window_after: float = 25.0,
) -> TSlot | None:
    candidates = [
        (position, slot)
        for position, slot in enumerate(slots)
        if slot.index not in used_indices
    ]
    if time_hint is not None:
        window_start = time_hint - window_before
        window_end = time_hint + window_after
        window_candidates = [
            (position, slot)
            for position, slot in candidates
            if slot.end >= window_start and slot.start <= window_end
        ]
        if window_candidates:
            candidates = sorted(window_candidates, key=lambda item: abs(item[1].start - time_hint))

    for _position, slot in candidates:
        haystack = normalize_match_text(slot.text)
        if needle in haystack or haystack in needle:
            return slot

    for start_position, slot in candidates:
        joined = normalize_match_text(slot.text)
        if not has_meaningful_overlap(joined, needle, min_partial_length):
            continue
        for next_slot in slots[start_position + 1 : start_position + max_joined_slots]:
            joined += normalize_match_text(next_slot.text)
            if needle in joined or joined in needle:
                return slot

    best_slot: TSlot | None = None
    best_score = 0.0
    for start_position, slot in candidates:
        joined = ""
        for next_slot in slots[start_position : start_position + max_joined_slots]:
            joined += normalize_match_text(next_slot.text)
            score = text_similarity(joined, needle)
            if time_hint is not None:
                distance = abs(slot.start - time_hint)
                score *= max(0.65, 1.0 - min(distance, window_after) / (window_after * 3))
            if score > best_score:
                best_score = score
                best_slot = slot
    if best_slot is not None and best_score >= min_similarity:
        return best_slot
    return None


def find_scene_start_slots(slots: list[TSlot], cues: list[SceneCue]) -> tuple[list[tuple[TSlot, SceneCue]], list[str]]:
    matches: list[tuple[TSlot, SceneCue]] = []
    missing: list[str] = []
    used_indices: set[int] = set()
    max_joined_slots = 4
    min_partial_length = 4
    min_similarity = 0.30

    for cue in cues:
        needle = normalize_match_text(cue.narration)
        if not needle:
            continue

        found = find_slot_by_text(
            slots,
            needle,
            used_indices,
            max_joined_slots,
            min_partial_length,
            min_similarity,
            time_hint=cue.start,
        )

        if found is None and cue.start is not None:
            found = find_slot_by_text(
                slots,
                needle,
                used_indices,
                max_joined_slots,
                min_partial_length,
                min_similarity,
            )

        if found is None and cue.start is not None:
            found = find_slot_by_time(slots, cue.start, used_indices)

        if found is None:
            missing.append(cue.narration)
            continue
        used_indices.add(found.index)
        matches.append((found, cue))

    return matches, missing


def _leading_number(path: Path) -> int | None:
    match = re.match(r"^(\d+)(?:[_\-\s.]|$)", path.stem)
    if not match:
        return None
    return int(match.group(1))


def filter_videos_from_start_number(videos: list[Path], start_number: int) -> list[Path]:
    if start_number <= 1:
        return videos
    numbered = [video for video in videos if (number := _leading_number(video)) is not None and number >= start_number]
    if numbered:
        return numbered
    offset = start_number - 1
    return videos[offset:] if offset < len(videos) else []
