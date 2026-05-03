import csv
import re
from pathlib import Path
from typing import Protocol, TypeVar


class SceneSlot(Protocol):
    index: int
    text: str


TSlot = TypeVar("TSlot", bound=SceneSlot)


def normalize_match_text(value: str) -> str:
    return re.sub(r"[\s\"'“”‘’.,!?…~·ㆍ:;()\[\]{}<>《》〈〉「」『』-]+", "", value).lower()


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


def read_scene_table(path: Path) -> list[str]:
    text = path.read_text(encoding="utf-8-sig", errors="replace")
    first_line = next((line for line in text.splitlines() if line.strip()), "")
    delimiter = "\t" if "\t" in first_line else ","
    reader = csv.DictReader(text.splitlines(), delimiter=delimiter)
    if not reader.fieldnames:
        raise ValueError("AI 장면표 헤더를 찾지 못했습니다.")

    narration_key = None
    for key in reader.fieldnames:
        cleaned = (key or "").strip().replace(" ", "")
        if cleaned in {"한글내레이션", "내레이션", "대사", "시작대사", "시작자막"}:
            narration_key = key
            break
    if narration_key is None:
        raise ValueError("AI 장면표에는 '한글 내레이션' 또는 '시작 자막' 열이 있어야 합니다.")

    narrations: list[str] = []
    for row in reader:
        narration = (row.get(narration_key) or "").strip().strip('"')
        if narration:
            narrations.append(narration)
    if not narrations:
        raise ValueError("AI 장면표에서 내레이션 문구를 찾지 못했습니다.")
    return narrations


def find_scene_start_slots(slots: list[TSlot], narrations: list[str]) -> tuple[list[tuple[TSlot, str]], list[str]]:
    matches: list[tuple[TSlot, str]] = []
    missing: list[str] = []
    used_indices: set[int] = set()
    max_joined_slots = 4
    min_partial_length = 4
    min_similarity = 0.30

    for narration in narrations:
        needle = normalize_match_text(narration)
        if not needle:
            continue
        found: TSlot | None = None

        for slot in slots:
            if slot.index in used_indices:
                continue
            haystack = normalize_match_text(slot.text)
            if needle in haystack or (
                len(haystack) >= min_partial_length and has_meaningful_overlap(haystack, needle, min_partial_length)
            ):
                found = slot
                break

        if found is None:
            for start_position, slot in enumerate(slots):
                if slot.index in used_indices:
                    continue
                joined = normalize_match_text(slot.text)
                if not has_meaningful_overlap(joined, needle, min_partial_length):
                    continue
                for next_slot in slots[start_position + 1 : start_position + max_joined_slots]:
                    joined += normalize_match_text(next_slot.text)
                    if needle in joined or joined in needle:
                        found = slot
                        break
                if found is not None:
                    break

        if found is None:
            best_slot: TSlot | None = None
            best_score = 0.0
            for start_position, slot in enumerate(slots):
                if slot.index in used_indices:
                    continue
                joined = ""
                for next_slot in slots[start_position : start_position + max_joined_slots]:
                    joined += normalize_match_text(next_slot.text)
                    score = text_similarity(joined, needle)
                    if score > best_score:
                        best_score = score
                        best_slot = slot
            if best_slot is not None and best_score >= min_similarity:
                found = best_slot

        if found is None:
            missing.append(narration)
            continue
        used_indices.add(found.index)
        matches.append((found, narration))

    matches.sort(key=lambda item: item[0].index)
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
