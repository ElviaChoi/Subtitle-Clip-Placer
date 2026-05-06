import json
import csv
import math
import os
import queue
import re
import shutil
import subprocess
import tempfile
import threading
from dataclasses import dataclass
from pathlib import Path
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from scene_matching import filter_videos_from_start_number, find_scene_start_slots, read_scene_table


VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".webm", ".avi", ".m4v"}
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".webp"}
MEDIA_EXTENSIONS = VIDEO_EXTENSIONS | IMAGE_EXTENSIONS


@dataclass
class Slot:
    index: int
    start: float
    end: float
    text: str

    @property
    def duration(self) -> float:
        return max(0.0, self.end - self.start)


@dataclass
class Caption:
    index: int
    start: float
    end: float
    text: str


@dataclass
class Placement:
    slot: Slot
    action: str
    video: Path | None
    effect: str = ""
    effect_duration: float | None = None


@dataclass
class RenderRun:
    action: str
    video: Path | None
    slots: list[Slot]
    effect: str = ""
    effect_duration: float | None = None

    @property
    def duration(self) -> float:
        return sum(slot.duration for slot in self.slots)

    @property
    def start(self) -> float:
        return self.slots[0].start

    @property
    def end(self) -> float:
        return self.slots[-1].end


class RenderCancelled(Exception):
    pass


def parse_srt_time(value: str) -> float:
    match = re.match(r"^\s*(\d+):(\d+):(\d+)[,.](\d+)\s*$", value)
    if not match:
        raise ValueError(f"잘못된 SRT 시간 형식입니다: {value}")
    hours, minutes, seconds, millis = match.groups()
    millis = (millis + "000")[:3]
    return (
        int(hours) * 3600
        + int(minutes) * 60
        + int(seconds)
        + int(millis) / 1000
    )


def read_srt_captions(path: Path) -> list[Caption]:
    text = path.read_text(encoding="utf-8-sig", errors="replace")
    blocks = re.split(r"\n\s*\n", text.strip())
    captions: list[Caption] = []
    time_pattern = re.compile(
        r"(\d{1,2}:\d{2}:\d{2}[,.]\d{1,3})\s*-->\s*"
        r"(\d{1,2}:\d{2}:\d{2}[,.]\d{1,3})"
    )

    for block in blocks:
        lines = [line.strip() for line in block.splitlines() if line.strip()]
        if not lines:
            continue
        time_line_index = None
        time_match = None
        for i, line in enumerate(lines):
            time_match = time_pattern.search(line)
            if time_match:
                time_line_index = i
                break
        if time_line_index is None or time_match is None:
            continue
        raw_number = lines[0] if time_line_index > 0 else str(len(captions) + 1)
        try:
            index = int(raw_number)
        except ValueError:
            index = len(captions) + 1
        caption_text = " ".join(lines[time_line_index + 1 :]).strip()
        captions.append(
            Caption(
                index=index,
                start=parse_srt_time(time_match.group(1)),
                end=parse_srt_time(time_match.group(2)),
                text=caption_text,
            )
        )

    captions = sorted(captions, key=lambda caption: caption.start)
    if not captions:
        raise ValueError("SRT에서 자막을 찾지 못했습니다.")
    return captions


def build_slots(
    captions: list[Caption],
    last_duration: float,
    start_index: int = 1,
    end_index: int | None = None,
) -> list[Slot]:
    if start_index < 1:
        raise ValueError("작업 시작 번호는 1 이상이어야 합니다.")
    if end_index is not None and end_index < start_index:
        raise ValueError("작업 종료 번호는 시작 번호보다 크거나 같아야 합니다.")

    all_slots: list[Slot] = []
    for index, caption in enumerate(captions):
        start = caption.start
        end = captions[index + 1].start if index + 1 < len(captions) else start + last_duration
        if end <= start:
            continue
        all_slots.append(Slot(index=index + 1, start=start, end=end, text=caption.text))
    if not all_slots:
        raise ValueError("유효한 자막 구간을 만들 수 없습니다.")
    slots = [
        slot
        for slot in all_slots
        if slot.index >= start_index and (end_index is None or slot.index <= end_index)
    ]
    if not slots:
        available = f"1-{all_slots[-1].index}"
        raise ValueError(f"작업 범위에 해당하는 자막이 없습니다. 사용 가능한 번호: {available}")
    return slots


def leading_number(path: Path) -> int | None:
    match = re.match(r"^(\d+)(?:[_\-\s.]|$)", path.stem)
    if not match:
        return None
    return int(match.group(1))


def seconds_to_text(seconds: float) -> str:
    millis = int(round(seconds * 1000))
    hours, rest = divmod(millis, 3600_000)
    minutes, rest = divmod(rest, 60_000)
    secs, ms = divmod(rest, 1000)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}.{ms:03d}"


def discover_videos(folder: Path) -> list[Path]:
    videos = [
        item
        for item in folder.iterdir()
        if item.is_file() and item.suffix.lower() in MEDIA_EXTENSIONS
    ]
    return sorted(
        videos,
        key=lambda item: (
            leading_number(item) is None,
            leading_number(item) if leading_number(item) is not None else 10**9,
            item.name.lower(),
        ),
    )


def match_videos_to_slots(videos: list[Path], slot_count: int) -> list[Path]:
    numbered: dict[int, Path] = {}
    duplicates: list[int] = []
    for video in videos:
        number = leading_number(video)
        if number is None:
            continue
        if number in numbered:
            duplicates.append(number)
        else:
            numbered[number] = video

    if numbered:
        if duplicates:
            duplicate_text = ", ".join(str(number) for number in sorted(set(duplicates)))
            raise ValueError(f"같은 앞번호를 가진 영상이 있습니다: {duplicate_text}")
        missing = [index for index in range(1, slot_count + 1) if index not in numbered]
        if missing:
            missing_text = ", ".join(str(index) for index in missing[:20])
            if len(missing) > 20:
                missing_text += "..."
            raise ValueError(f"앞번호가 빠진 영상이 있습니다: {missing_text}")
        return [numbered[index] for index in range(1, slot_count + 1)]

    if len(videos) < slot_count:
        raise ValueError(f"영상 파일이 부족합니다. 자막 구간 {slot_count}개, 영상 {len(videos)}개입니다.")
    return videos[:slot_count]


def loose_match_video(video_folder: Path, value: str) -> Path:
    raw = value.strip().strip('"')
    if not raw:
        raise ValueError("비어 있는 영상파일 값입니다.")
    candidate = Path(raw)
    if candidate.is_absolute():
        return candidate
    direct = video_folder / raw
    if direct.exists():
        return direct
    for video in discover_videos(video_folder):
        if video.name == raw or video.stem == raw:
            return video
    return direct


def normalize_action(value: str, has_video: bool) -> str:
    raw = value.strip().lower().replace(" ", "")
    if raw in {"", "자동"}:
        return "video" if has_video else "auto"
    if raw in {"영상", "새영상", "이미지", "사진", "video", "clip", "image", "photo"}:
        return "video"
    if raw in {"이전유지", "유지", "계속", "previous", "prev", "hold"}:
        return "hold"
    if raw in {"검은화면", "빈화면", "없음", "건너뜀", "blank", "black", "none", "skip"}:
        return "blank"
    raise ValueError(f"지원하지 않는 작업 값입니다: {value}")


def normalize_effect(value: str) -> str:
    raw = value.strip().lower().replace(" ", "")
    if raw in {"", "없음", "none", "no", "off"}:
        return ""
    if raw in {"부드럽게", "smooth", "soft", "gentle"}:
        return "부드럽게"
    if raw in {"움직임", "이동", "패닝", "pan", "move"}:
        return "움직임"
    if raw in {"강조", "emphasis", "highlight", "impact"}:
        return "강조"
    if raw in {"줌인", "확대", "zoomin"}:
        return "줌인"
    if raw in {"줌아웃", "축소", "zoomout"}:
        return "줌아웃"
    if raw in {"페이드", "fade", "fadeinout"}:
        return "페이드"
    raise ValueError(f"지원하지 않는 효과 값입니다: {value}")


def parse_optional_seconds(value: str, label: str) -> float | None:
    raw = value.strip()
    if not raw:
        return None
    try:
        seconds = float(raw)
    except ValueError as exc:
        raise ValueError(f"{label} 값이 숫자가 아닙니다: {value}") from exc
    if seconds <= 0:
        raise ValueError(f"{label} 값은 0보다 커야 합니다: {value}")
    return seconds


def effect_help_text() -> str:
    return "이미지 효과: 없음 / 부드럽게 / 움직임 / 강조 / 줌인 / 줌아웃 / 페이드"


def effect_help_detail() -> str:
    return (
        "CSV의 '효과' 칸에 아래 값 중 하나를 입력합니다.\n\n"
        "추천 효과\n"
        "- 없음: 정지 이미지\n"
        "- 부드럽게: 약한 줌인 + 페이드\n"
        "- 움직임: 좌우로 천천히 이동\n"
        "- 강조: 초반 빠른 줌인 + 짧은 페이드인\n\n"
        "직접 효과\n"
        "- 줌인: 지정 시간 동안 확대\n"
        "- 줌아웃: 지정 시간 동안 축소\n"
        "- 페이드: 시작/끝 페이드\n\n"
        "'효과시간초' 칸은 비워두면 자동값을 사용합니다.\n"
        "필요할 때만 0.8, 1, 2 같은 초 단위 숫자를 입력하세요."
    )


def effect_tooltip_text() -> str:
    return (
        "효과 칸 예시\n"
        "부드럽게: 약한 줌인 + 페이드\n"
        "움직임: 좌우 이동\n"
        "강조: 초반 빠른 줌인\n"
        "효과시간초는 비우면 자동"
    )


def display_effect(effect: str, effect_duration: float | None = None) -> str:
    if not effect:
        return ""
    if effect_duration is None:
        return effect
    return f"{effect} ({effect_duration:g}s)"


def is_image_file(path: Path | None) -> bool:
    return path is not None and path.suffix.lower() in IMAGE_EXTENSIONS


def display_action(placement: Placement) -> str:
    if placement.action == "video" and is_image_file(placement.video):
        return "이미지"
    return action_label(placement.action)


def action_label(action: str) -> str:
    if action == "video":
        return "영상"
    if action == "hold":
        return "이전유지"
    if action == "blank":
        return "검은화면"
    return action


def read_csv_placement_overrides(csv_path: Path, video_folder: Path) -> dict[int, tuple[str, Path | None, str, float | None]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        if not reader.fieldnames:
            raise ValueError("CSV 헤더를 찾지 못했습니다.")
        number_key = "번호" if "번호" in reader.fieldnames else None
        video_key = "영상파일" if "영상파일" in reader.fieldnames else None
        action_key = "작업" if "작업" in reader.fieldnames else None
        effect_key = "효과" if "효과" in reader.fieldnames else None
        effect_duration_key = "효과시간초" if "효과시간초" in reader.fieldnames else None
        if number_key is None or video_key is None:
            raise ValueError("CSV에는 '번호'와 '영상파일' 열이 있어야 합니다.")

        mapping: dict[int, tuple[str, Path | None, str, float | None]] = {}
        for row in reader:
            raw_number = (row.get(number_key) or "").strip()
            raw_video = (row.get(video_key) or "").strip()
            raw_action = (row.get(action_key) or "").strip() if action_key else ""
            raw_effect = (row.get(effect_key) or "").strip() if effect_key else ""
            raw_effect_duration = (row.get(effect_duration_key) or "").strip() if effect_duration_key else ""
            if not raw_number:
                continue
            try:
                number = int(raw_number)
            except ValueError as exc:
                raise ValueError(f"CSV 번호가 숫자가 아닙니다: {raw_number}") from exc
            action = normalize_action(raw_action, bool(raw_video))
            if action == "auto":
                continue
            video = loose_match_video(video_folder, raw_video) if raw_video else None
            if action == "video" and video is None:
                # In Excel, users often leave 영상파일 blank to continue the previous clip.
                action = "hold"
            effect = normalize_effect(raw_effect) if action == "video" and is_image_file(video) else ""
            effect_duration = parse_optional_seconds(raw_effect_duration, "효과시간초") if effect else None
            mapping[number] = (action, video, effect, effect_duration)
    return mapping


def build_scene_table_placements(slots: list[Slot], videos: list[Path], cues) -> tuple[list[Placement], list[str]]:
    matches, missing = find_scene_start_slots(slots, cues)
    if not matches:
        return [Placement(slot=slot, action="blank", video=None) for slot in slots], [cue.narration for cue in cues]

    available_matches = matches[: len(videos)]
    if len(videos) < len(matches):
        missing.extend(cue.narration for _slot, cue in matches[len(videos) :])

    starts_by_index = {slot.index: videos[position] for position, (slot, _cue) in enumerate(available_matches)}
    placements: list[Placement] = []
    has_started = False
    for slot in slots:
        video = starts_by_index.get(slot.index)
        if video is not None:
            placements.append(Placement(slot=slot, action="video", video=video))
            has_started = True
        elif has_started:
            placements.append(Placement(slot=slot, action="hold", video=None))
        else:
            placements.append(Placement(slot=slot, action="blank", video=None))
    return placements, missing


def build_placements(
    slots: list[Slot],
    videos: list[Path],
    video_folder: Path,
    csv_path: Path | None = None,
) -> list[Placement]:
    slot_count = len(slots)
    video_by_number: dict[int, Path] = {}
    duplicates: list[int] = []
    for video in videos:
        number = leading_number(video)
        if number is None:
            continue
        if number in video_by_number:
            duplicates.append(number)
        else:
            video_by_number[number] = video

    if duplicates:
        duplicate_text = ", ".join(str(number) for number in sorted(set(duplicates)))
        raise ValueError(f"같은 앞번호를 가진 영상이 있습니다: {duplicate_text}")

    if video_by_number:
        matched: list[Path | None] = [video_by_number.get(slot.index) for slot in slots]
    else:
        matched = [
            videos[position - 1] if position - 1 < len(videos) else None
            for position in range(1, slot_count + 1)
        ]

    placements = [
        Placement(slot=slot, action="video" if video else "blank", video=video)
        for slot, video in zip(slots, matched)
    ]

    if csv_path and csv_path.exists():
        mapping = read_csv_placement_overrides(csv_path, video_folder)
        slot_positions = {slot.index: position for position, slot in enumerate(slots)}
        for index, (action, video, effect, effect_duration) in mapping.items():
            position = slot_positions.get(index)
            if position is not None:
                placements[position] = Placement(
                    slot=slots[position],
                    action=action,
                    video=video,
                    effect=effect,
                    effect_duration=effect_duration,
                )
    return placements


def write_work_csv(path: Path, placements: list[Placement]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["번호", "시작시간", "길이초", "대사", "작업", "영상파일", "효과", "효과시간초"],
        )
        writer.writeheader()
        for placement in placements:
            slot = placement.slot
            writer.writerow(
                {
                    "번호": slot.index,
                    "시작시간": seconds_to_text(slot.start),
                    "길이초": f"{slot.duration:.3f}",
                    "대사": slot.text,
                    "작업": action_label(placement.action),
                    "영상파일": placement.video.name if placement.video else "",
                    "효과": placement.effect,
                    "효과시간초": f"{placement.effect_duration:g}" if placement.effect_duration is not None else "",
                }
            )


def build_render_runs(placements: list[Placement]) -> list[RenderRun]:
    runs: list[RenderRun] = []
    current: RenderRun | None = None

    for placement in placements:
        if placement.action == "hold":
            if current is None or current.action != "video" or current.video is None:
                raise ValueError(f"{placement.slot.index}번 대사는 이전에 유지할 영상이 없습니다.")
            current.slots.append(placement.slot)
            continue

        if placement.action == "blank":
            if current is not None and current.action == "blank":
                current.slots.append(placement.slot)
            else:
                current = RenderRun(action="blank", video=None, slots=[placement.slot])
                runs.append(current)
            continue

        if placement.action == "video":
            if placement.video is None:
                raise ValueError(f"{placement.slot.index}번 대사는 영상파일이 필요합니다.")
            current = RenderRun(
                action="video",
                video=placement.video,
                slots=[placement.slot],
                effect=placement.effect,
                effect_duration=placement.effect_duration,
            )
            runs.append(current)
            continue

        raise ValueError(f"지원하지 않는 작업입니다: {placement.action}")

    return runs


def ffmpeg_pair(ffmpeg_text: str) -> tuple[str, str]:
    ffmpeg_text = ffmpeg_text.strip().strip('"')
    if ffmpeg_text:
        ffmpeg_path = Path(ffmpeg_text)
        ffprobe_path = ffmpeg_path.with_name("ffprobe.exe")
        return str(ffmpeg_path), str(ffprobe_path)
    return "ffmpeg", "ffprobe"


def run_process(
    command: list[str],
    log,
    cancel_event: threading.Event | None = None,
    set_current_process=None,
) -> None:
    if cancel_event is not None and cancel_event.is_set():
        raise RenderCancelled("작업이 중지되었습니다.")
    pretty = " ".join(f'"{part}"' if " " in part else part for part in command)
    log(pretty)
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
    )
    if set_current_process is not None:
        set_current_process(process)
    try:
        assert process.stdout is not None
        for line in process.stdout:
            if cancel_event is not None and cancel_event.is_set():
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                raise RenderCancelled("작업이 중지되었습니다.")
            line = line.strip()
            if line:
                log(line)
        return_code = process.wait()
        if cancel_event is not None and cancel_event.is_set():
            raise RenderCancelled("작업이 중지되었습니다.")
        if return_code != 0:
            raise RuntimeError(f"FFmpeg 실행 실패, 종료 코드: {return_code}")
    finally:
        if set_current_process is not None:
            set_current_process(None)


def get_video_duration(ffprobe: str, video: Path) -> float:
    command = [
        ffprobe,
        "-v",
        "error",
        "-show_entries",
        "format=duration",
        "-of",
        "json",
        str(video),
    ]
    process = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
    )
    if process.returncode != 0:
        raise RuntimeError(f"영상 길이 확인 실패: {video.name}\n{process.stderr}")
    data = json.loads(process.stdout)
    duration = float(data["format"]["duration"])
    if duration <= 0:
        raise RuntimeError(f"영상 길이가 0초입니다: {video.name}")
    return duration


def aspect_size(aspect: str) -> tuple[int, int]:
    if aspect == "세로 쇼츠 (1080x1920)":
        return 1080, 1920
    if aspect == "가로 영상 (1920x1080)":
        return 1920, 1080
    raise ValueError("지원하지 않는 화면 비율입니다.")


def normalize_filter(width: int, height: int, extra: str | None = None) -> str:
    base = (
        f"scale={width}:{height}:force_original_aspect_ratio=increase,"
        f"crop={width}:{height},fps=30,format=yuv420p"
    )
    if extra:
        return f"{extra},{base}"
    return base


def clamp_effect_seconds(duration: float, effect_duration: float | None, default_seconds: float) -> float:
    seconds = default_seconds if effect_duration is None else effect_duration
    return max(1 / 30, min(duration, seconds))


def fade_filter(duration: float, effect_duration: float | None = None, default_max: float = 0.6) -> str:
    if effect_duration is None:
        fade_duration = min(default_max, max(0.1, duration / 3))
    else:
        fade_duration = effect_duration
    fade_duration = max(1 / 30, min(duration / 2, fade_duration))
    fade_out_start = max(0.0, duration - fade_duration)
    return (
        f"fade=t=in:st=0:d={fade_duration:.3f},"
        f"fade=t=out:st={fade_out_start:.3f}:d={fade_duration:.3f}"
    )


def image_filter(width: int, height: int, duration: float, effect: str, effect_duration: float | None = None) -> str:
    frames = max(1, int(math.ceil(duration * 30)))
    effect_seconds = clamp_effect_seconds(duration, effect_duration, duration)
    effect_frames = max(1, min(frames, int(math.ceil(effect_seconds * 30))))
    large_width = width * 2
    large_height = height * 2
    fit_large = (
        f"scale={large_width}:{large_height}:force_original_aspect_ratio=increase,"
        f"crop={large_width}:{large_height}"
    )
    center = "x='iw/2-(iw/zoom/2)':y='ih/2-(ih/zoom/2)'"

    if effect == "줌인":
        return (
            f"{fit_large},"
            f"zoompan=z='min(1+0.12*min(on,{effect_frames})/{effect_frames},1.12)':{center}:d={frames}:s={width}x{height}:fps=30,"
            "format=yuv420p"
        )
    if effect == "줌아웃":
        return (
            f"{fit_large},"
            f"zoompan=z='max(1.12-0.12*min(on,{effect_frames})/{effect_frames},1)':{center}:d={frames}:s={width}x{height}:fps=30,"
            "format=yuv420p"
        )
    if effect == "부드럽게":
        fade = fade_filter(duration, default_max=0.5)
        return (
            f"{fit_large},"
            f"zoompan=z='min(1+0.08*min(on,{effect_frames})/{effect_frames},1.08)':{center}:d={frames}:s={width}x{height}:fps=30,"
            f"{fade},format=yuv420p"
        )
    if effect == "움직임":
        pan_seconds = clamp_effect_seconds(duration, effect_duration, duration)
        pan_frames = max(1, min(frames, int(math.ceil(pan_seconds * 30))))
        x = f"x='(iw-iw/zoom)*min(on,{pan_frames})/{pan_frames}'"
        y = "y='ih/2-(ih/zoom/2)'"
        return (
            f"{fit_large},"
            f"zoompan=z='1.12':{x}:{y}:d={frames}:s={width}x{height}:fps=30,"
            "format=yuv420p"
        )
    if effect == "강조":
        emphasis_seconds = clamp_effect_seconds(duration, effect_duration, min(1.0, duration))
        emphasis_frames = max(1, min(frames, int(math.ceil(emphasis_seconds * 30))))
        fade_in = min(0.2, duration / 2)
        return (
            f"{fit_large},"
            f"zoompan=z='min(1+0.15*min(on,{emphasis_frames})/{emphasis_frames},1.15)':{center}:d={frames}:s={width}x{height}:fps=30,"
            f"fade=t=in:st=0:d={fade_in:.3f},format=yuv420p"
        )

    base = normalize_filter(width, height)
    if effect == "페이드":
        return f"{base},{fade_filter(duration, effect_duration)}"
    return base


def mode_for_slot(mode: str, slot_duration: float, video_duration: float, threshold: float) -> str:
    if video_duration >= slot_duration:
        return "trim"
    if mode == "반복 후 자르기":
        return "loop"
    if mode == "느리게 늘리기":
        return "slow"
    if slot_duration <= video_duration * threshold:
        return "slow"
    return "loop"


def render_image_segment(
    ffmpeg: str,
    image: Path,
    output: Path,
    slot_duration: float,
    width: int,
    height: int,
    effect: str,
    effect_duration: float | None,
    log,
    cancel_event: threading.Event | None = None,
    set_current_process=None,
) -> str:
    vf = image_filter(width, height, slot_duration, effect, effect_duration)
    command = [
        ffmpeg,
        "-y",
        "-loop",
        "1",
        "-i",
        str(image),
        "-t",
        f"{slot_duration:.3f}",
        "-vf",
        vf,
        "-an",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "18",
        "-movflags",
        "+faststart",
        str(output),
    ]
    run_process(command, log, cancel_event, set_current_process)
    effect_text = display_effect(effect, effect_duration) if effect else "효과 없음"
    return f"이미지 {effect_text} ({slot_duration:.2f}s)"


def render_segment(
    ffmpeg: str,
    ffprobe: str,
    video: Path,
    output: Path,
    slot_duration: float,
    width: int,
    height: int,
    mode: str,
    threshold: float,
    log,
    cancel_event: threading.Event | None = None,
    set_current_process=None,
) -> str:
    video_duration = get_video_duration(ffprobe, video)
    selected = mode_for_slot(mode, slot_duration, video_duration, threshold)

    if selected == "slow":
        factor = slot_duration / video_duration
        vf = normalize_filter(width, height, f"setpts={factor:.8f}*PTS")
        command = [
            ffmpeg,
            "-y",
            "-i",
            str(video),
            "-t",
            f"{slot_duration:.3f}",
            "-vf",
            vf,
            "-an",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "18",
            "-movflags",
            "+faststart",
            str(output),
        ]
        run_process(command, log, cancel_event, set_current_process)
        return f"느리게 늘리기 ({video_duration:.2f}s -> {slot_duration:.2f}s)"

    if selected == "loop":
        vf = normalize_filter(width, height)
        command = [
            ffmpeg,
            "-y",
            "-stream_loop",
            "-1",
            "-i",
            str(video),
            "-t",
            f"{slot_duration:.3f}",
            "-vf",
            vf,
            "-an",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "18",
            "-movflags",
            "+faststart",
            str(output),
        ]
        run_process(command, log, cancel_event, set_current_process)
        repeats = math.ceil(slot_duration / video_duration)
        return f"반복 후 자르기 ({video_duration:.2f}s x {repeats}회 -> {slot_duration:.2f}s)"

    vf = normalize_filter(width, height)
    command = [
        ffmpeg,
        "-y",
        "-i",
        str(video),
        "-t",
        f"{slot_duration:.3f}",
        "-vf",
        vf,
        "-an",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "18",
        "-movflags",
        "+faststart",
        str(output),
    ]
    run_process(command, log, cancel_event, set_current_process)
    return f"자르기 ({video_duration:.2f}s -> {slot_duration:.2f}s)"


def render_black_segment(
    ffmpeg: str,
    output: Path,
    duration: float,
    width: int,
    height: int,
    log,
    cancel_event: threading.Event | None = None,
    set_current_process=None,
) -> str:
    command = [
        ffmpeg,
        "-y",
        "-f",
        "lavfi",
        "-i",
        f"color=c=black:s={width}x{height}:r=30",
        "-t",
        f"{duration:.3f}",
        "-an",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "18",
        "-pix_fmt",
        "yuv420p",
        "-movflags",
        "+faststart",
        str(output),
    ]
    run_process(command, log, cancel_event, set_current_process)
    return f"검은화면 ({duration:.2f}s)"


def concat_segments(
    ffmpeg: str,
    segments: list[Path],
    output: Path,
    temp_dir: Path,
    log,
    cancel_event: threading.Event | None = None,
    set_current_process=None,
) -> None:
    concat_file = temp_dir / "concat.txt"
    lines = []
    for segment in segments:
        safe_path = str(segment).replace("\\", "/").replace("'", "'\\''")
        lines.append(f"file '{safe_path}'")
    concat_file.write_text("\n".join(lines), encoding="utf-8")
    command = [
        ffmpeg,
        "-y",
        "-f",
        "concat",
        "-safe",
        "0",
        "-i",
        str(concat_file),
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "18",
        "-pix_fmt",
        "yuv420p",
        "-an",
        "-movflags",
        "+faststart",
        str(output),
    ]
    run_process(command, log, cancel_event, set_current_process)


def build_video(
    srt_path: Path,
    video_folder: Path,
    output_path: Path,
    csv_path: Path | None,
    ffmpeg_text: str,
    aspect: str,
    mode: str,
    threshold: float,
    last_duration: float,
    start_index: int,
    end_index: int | None,
    keep_temp: bool,
    log,
    cancel_event: threading.Event | None = None,
    set_current_process=None,
) -> None:
    ffmpeg, ffprobe = ffmpeg_pair(ffmpeg_text)
    if ffmpeg_text.strip():
        if not Path(ffmpeg).exists():
            raise FileNotFoundError(f"FFmpeg 파일을 찾을 수 없습니다: {ffmpeg}")
        if not Path(ffprobe).exists():
            raise FileNotFoundError(f"ffprobe.exe를 찾을 수 없습니다: {ffprobe}")
    else:
        if shutil.which("ffmpeg") is None or shutil.which("ffprobe") is None:
            raise FileNotFoundError(
                "FFmpeg가 PATH에 없습니다. GUI에서 ffmpeg.exe 위치를 직접 선택하세요."
            )

    captions = read_srt_captions(srt_path)
    slots = build_slots(captions, last_duration, start_index, end_index)
    discovered_videos = discover_videos(video_folder)
    placements = build_placements(slots, discovered_videos, video_folder, csv_path)
    runs = build_render_runs(placements)
    videos = [run.video for run in runs if run.video is not None]

    missing_files = [video for video in videos if not video.exists()]
    if missing_files:
        names = ", ".join(str(video) for video in missing_files[:5])
        if len(missing_files) > 5:
            names += "..."
        raise FileNotFoundError(f"영상 파일을 찾을 수 없습니다: {names}")

    width, height = aspect_size(aspect)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    temp_root = Path(tempfile.mkdtemp(prefix="subtitle_clip_placer_"))
    log(f"임시 폴더: {temp_root}")
    try:
        segments: list[Path] = []
        for run_index, run in enumerate(runs, start=1):
            if cancel_event is not None and cancel_event.is_set():
                raise RenderCancelled("작업이 중지되었습니다.")
            segment = temp_root / f"segment_{run_index:04d}.mp4"
            log("")
            slot_numbers = f"{run.slots[0].index}"
            if len(run.slots) > 1:
                slot_numbers = f"{run.slots[0].index}-{run.slots[-1].index}"
            if run.action == "blank":
                log(
                    f"[{run_index}/{len(runs)}] {slot_numbers}번 대사 -> "
                    f"검은화면 {run.start:.2f}s~{run.end:.2f}s ({run.duration:.2f}s)"
                )
                result = render_black_segment(
                    ffmpeg=ffmpeg,
                    output=segment,
                    duration=run.duration,
                    width=width,
                    height=height,
                    log=log,
                    cancel_event=cancel_event,
                    set_current_process=set_current_process,
                )
            else:
                assert run.video is not None
                media_type = "이미지" if is_image_file(run.video) else "영상"
                effect_text = f", 효과: {display_effect(run.effect, run.effect_duration)}" if is_image_file(run.video) and run.effect else ""
                log(
                    f"[{run_index}/{len(runs)}] {slot_numbers}번 대사 -> {media_type} {run.video.name} "
                    f"{run.start:.2f}s~{run.end:.2f}s ({run.duration:.2f}s){effect_text}"
                )
                if is_image_file(run.video):
                    result = render_image_segment(
                        ffmpeg=ffmpeg,
                        image=run.video,
                        output=segment,
                        slot_duration=run.duration,
                        width=width,
                        height=height,
                        effect=run.effect,
                        effect_duration=run.effect_duration,
                        log=log,
                        cancel_event=cancel_event,
                        set_current_process=set_current_process,
                    )
                else:
                    result = render_segment(
                        ffmpeg=ffmpeg,
                        ffprobe=ffprobe,
                        video=run.video,
                        output=segment,
                        slot_duration=run.duration,
                        width=width,
                        height=height,
                        mode=mode,
                        threshold=threshold,
                        log=log,
                        cancel_event=cancel_event,
                        set_current_process=set_current_process,
                    )
            log(f"처리 방식: {result}")
            segments.append(segment)

        log("")
        log("최종 영상을 합치는 중입니다.")
        concat_segments(ffmpeg, segments, output_path, temp_root, log, cancel_event, set_current_process)
        log("")
        log(f"완료: {output_path}")
    finally:
        if keep_temp:
            log(f"임시 파일 보관: {temp_root}")
        else:
            shutil.rmtree(temp_root, ignore_errors=True)


class Tooltip:
    def __init__(self, widget: tk.Widget, text: str, delay_ms: int = 450) -> None:
        self.widget = widget
        self.text = text
        self.delay_ms = delay_ms
        self.after_id: str | None = None
        self.tip: tk.Toplevel | None = None
        widget.bind("<Enter>", self.schedule, add="+")
        widget.bind("<Leave>", self.hide, add="+")
        widget.bind("<ButtonPress>", self.hide, add="+")

    def schedule(self, _event=None) -> None:
        self.cancel()
        self.after_id = self.widget.after(self.delay_ms, self.show)

    def cancel(self) -> None:
        if self.after_id is not None:
            self.widget.after_cancel(self.after_id)
            self.after_id = None

    def show(self) -> None:
        self.after_id = None
        if self.tip is not None:
            return
        x = self.widget.winfo_rootx() + 12
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 8
        self.tip = tk.Toplevel(self.widget)
        self.tip.wm_overrideredirect(True)
        self.tip.wm_geometry(f"+{x}+{y}")
        frame = tk.Frame(self.tip, background="#1f2937", borderwidth=1, relief=tk.SOLID)
        frame.pack()
        tk.Label(
            frame,
            text=self.text,
            justify=tk.LEFT,
            background="#1f2937",
            foreground="#ffffff",
            padx=14,
            pady=12,
            font=("Malgun Gothic", 11),
        ).pack()

    def hide(self, _event=None) -> None:
        self.cancel()
        if self.tip is not None:
            self.tip.destroy()
            self.tip = None


class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Subtitle Clip Placer")
        self.geometry("1080x860")
        self.minsize(940, 720)
        self.configure(bg="#f3f5f7")

        self.log_queue: queue.Queue[str] = queue.Queue()
        self.worker: threading.Thread | None = None
        self.last_error: str | None = None
        self.cancel_event = threading.Event()
        self.process_lock = threading.Lock()
        self.current_process: subprocess.Popen | None = None

        self.status_var = tk.StringVar(value="대기 중")
        self.srt_var = tk.StringVar()
        self.video_dir_var = tk.StringVar()
        self.output_var = tk.StringVar()
        self.csv_var = tk.StringVar()
        self.ffmpeg_var = tk.StringVar(value=self.default_ffmpeg_path())
        self.aspect_var = tk.StringVar(value="가로 영상 (1920x1080)")
        self.mode_var = tk.StringVar(value="반복 후 자르기")
        self.threshold_var = tk.DoubleVar(value=1.2)
        self.last_duration_var = tk.DoubleVar(value=8.0)
        self.range_start_var = tk.IntVar(value=1)
        self.range_end_var = tk.StringVar()
        self.video_start_number_var = tk.IntVar(value=1)
        self.keep_temp_var = tk.BooleanVar(value=False)

        self.configure_styles()
        self.create_widgets()
        self.mode_var.trace_add("write", self.update_mode_option_state)
        self.update_mode_option_state()
        self.after(100, self.drain_log_queue)

    def default_ffmpeg_path(self) -> str:
        app_dir = Path(__file__).resolve().parent
        for candidate in (
            app_dir / "ffmpeg" / "bin" / "ffmpeg.exe",
            app_dir / "ffmpeg.exe",
        ):
            if candidate.exists():
                return str(candidate)
        return ""

    def configure_styles(self) -> None:
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass
        self.option_add("*TCombobox*Listbox.font", ("Malgun Gothic", 11))
        self.option_add("*TCombobox*Listbox.selectBorderWidth", 2)
        style.configure("App.TFrame", background="#f3f5f7")
        style.configure("Panel.TLabelframe", background="#ffffff", bordercolor="#d8dee7")
        style.configure(
            "Panel.TLabelframe.Label",
            background="#ffffff",
            foreground="#243043",
            font=("Malgun Gothic", 10, "bold"),
        )
        style.configure("App.TLabel", background="#f3f5f7", foreground="#243043")
        style.configure("Guide.TLabel", background="#f3f5f7", foreground="#223047", font=("Malgun Gothic", 9, "bold"))
        style.configure("Panel.TFrame", background="#ffffff")
        style.configure("Panel.TLabel", background="#ffffff", foreground="#243043")
        style.configure("Muted.TLabel", background="#ffffff", foreground="#697386")
        style.configure("Status.TLabel", background="#f3f5f7", foreground="#1f5f9f", font=("Malgun Gothic", 10, "bold"))
        style.configure("TEntry", padding=(8, 6), font=("Malgun Gothic", 10))
        style.configure("TCombobox", padding=(8, 7), arrowsize=18, font=("Malgun Gothic", 10))
        style.configure("TSpinbox", padding=(8, 6), arrowsize=14, font=("Malgun Gothic", 10))
        style.configure("TButton", padding=(12, 7), font=("Malgun Gothic", 9))
        style.configure(
            "Primary.TButton",
            padding=(18, 8),
            font=("Malgun Gothic", 10, "bold"),
        )
        style.configure("Treeview", rowheight=26, font=("Malgun Gothic", 9))
        style.configure("Treeview.Heading", font=("Malgun Gothic", 9, "bold"))

    def create_widgets(self) -> None:
        root = ttk.Frame(self, padding=18, style="App.TFrame")
        root.pack(fill=tk.BOTH, expand=True)

        header = tk.Frame(root, bg="#182235", height=62)
        header.pack(fill=tk.X, pady=(0, 10))
        header.pack_propagate(False)
        tk.Label(
            header,
            text="Subtitle Clip Placer",
            bg="#182235",
            fg="#ffffff",
            font=("Malgun Gothic", 16, "bold"),
        ).pack(anchor=tk.W, padx=16, pady=(9, 0))
        tk.Label(
            header,
            text="SRT 대사 기준으로 영상 클립을 배치하고 Excel 작업표로 매칭을 관리합니다.",
            bg="#182235",
            fg="#b9c3d4",
            font=("Malgun Gothic", 9),
        ).pack(anchor=tk.W, padx=17, pady=(2, 0))

        form = ttk.LabelFrame(root, text="파일과 출력 설정", padding=10, style="Panel.TLabelframe")
        form.pack(fill=tk.X)
        form.columnconfigure(0, minsize=132)
        form.columnconfigure(1, weight=1)

        self.add_path_row(form, 0, "SRT 파일", self.srt_var, self.browse_srt)
        self.add_path_row(form, 1, "영상 폴더", self.video_dir_var, self.browse_video_dir)
        self.add_path_row(form, 2, "저장 위치/파일명", self.output_var, self.browse_output)
        self.add_path_row(form, 3, "CSV 매핑(선택)", self.csv_var, self.browse_csv)
        self.add_path_row(form, 4, "ffmpeg.exe", self.ffmpeg_var, self.browse_ffmpeg)

        ttk.Label(form, text="화면 비율", style="Panel.TLabel").grid(
            row=5, column=0, sticky=tk.W, padx=(0, 18), pady=8
        )
        aspect = ttk.Combobox(
            form,
            textvariable=self.aspect_var,
            values=["세로 쇼츠 (1080x1920)", "가로 영상 (1920x1080)"],
            state="readonly",
            font=("Malgun Gothic", 10),
        )
        aspect.grid(row=5, column=1, sticky=tk.EW, pady=8)

        ttk.Label(form, text="부족한 영상 처리", style="Panel.TLabel").grid(
            row=6, column=0, sticky=tk.W, padx=(0, 18), pady=8
        )
        mode = ttk.Combobox(
            form,
            textvariable=self.mode_var,
            values=["자동", "반복 후 자르기", "느리게 늘리기"],
            state="readonly",
            font=("Malgun Gothic", 10),
        )
        mode.grid(row=6, column=1, sticky=tk.EW, pady=8)

        ttk.Label(form, text="세부 옵션", style="Panel.TLabel").grid(
            row=7, column=0, sticky=tk.NW, padx=(0, 18), pady=(8, 6)
        )
        options = ttk.Frame(form, style="Panel.TFrame")
        options.grid(row=7, column=1, sticky=tk.EW, pady=(6, 8))
        options.columnconfigure(0, weight=1)
        options.columnconfigure(1, weight=1)

        auto_options = ttk.LabelFrame(options, text="자동 모드", padding=10, style="Panel.TLabelframe")
        auto_options.grid(row=0, column=0, sticky=tk.EW, padx=(0, 8))
        auto_options.columnconfigure(1, weight=1)
        ttk.Label(auto_options, text="느리게 허용 배율", style="Panel.TLabel").grid(
            row=0, column=0, sticky=tk.W, padx=(0, 8)
        )
        self.threshold_spinbox = ttk.Spinbox(
            auto_options,
            from_=1.01,
            to=2.0,
            increment=0.05,
            textvariable=self.threshold_var,
            width=8,
            font=("Malgun Gothic", 10),
        )
        self.threshold_spinbox.grid(row=0, column=1, sticky=tk.EW)
        self.threshold_hint = ttk.Label(
            auto_options,
            text="1.2 = 원본 길이의 120%까지 느리게",
            style="Muted.TLabel",
        )
        self.threshold_hint.grid(row=1, column=0, columnspan=2, sticky=tk.W, pady=(6, 0))

        common_options = ttk.LabelFrame(options, text="공통", padding=10, style="Panel.TLabelframe")
        common_options.grid(row=0, column=1, sticky=tk.EW)
        common_options.columnconfigure(1, weight=1)
        common_options.columnconfigure(3, weight=1)
        ttk.Label(common_options, text="마지막 자막 길이", style="Panel.TLabel").grid(
            row=0, column=0, sticky=tk.W, padx=(0, 8)
        )
        ttk.Spinbox(
            common_options,
            from_=1.0,
            to=30.0,
            increment=0.5,
            textvariable=self.last_duration_var,
            width=8,
            font=("Malgun Gothic", 10),
        ).grid(row=0, column=1, sticky=tk.EW)
        ttk.Label(common_options, text="영상 시작 번호", style="Panel.TLabel").grid(
            row=0, column=2, sticky=tk.W, padx=(14, 8)
        )
        ttk.Spinbox(
            common_options,
            from_=1,
            to=999,
            increment=1,
            textvariable=self.video_start_number_var,
            width=8,
            font=("Malgun Gothic", 10),
        ).grid(row=0, column=3, sticky=tk.EW)
        ttk.Label(common_options, text="작업 시작 번호", style="Panel.TLabel").grid(
            row=1, column=0, sticky=tk.W, padx=(0, 8), pady=(8, 0)
        )
        ttk.Spinbox(
            common_options,
            from_=1,
            to=9999,
            increment=1,
            textvariable=self.range_start_var,
            width=8,
            font=("Malgun Gothic", 10),
        ).grid(row=1, column=1, sticky=tk.EW, pady=(8, 0))
        ttk.Label(common_options, text="작업 종료 번호", style="Panel.TLabel").grid(
            row=1, column=2, sticky=tk.W, padx=(14, 8), pady=(8, 0)
        )
        ttk.Entry(
            common_options,
            textvariable=self.range_end_var,
            width=8,
            font=("Malgun Gothic", 10),
        ).grid(row=1, column=3, sticky=tk.EW, pady=(8, 0))
        ttk.Checkbutton(
            common_options,
            text="임시 파일 보관",
            variable=self.keep_temp_var,
        ).grid(row=2, column=0, columnspan=4, sticky=tk.W, pady=(8, 0))

        actions = ttk.Frame(root, style="App.TFrame")
        actions.pack(fill=tk.X, pady=(8, 8))
        ttk.Button(actions, text="1. 미리보기 확인", command=self.refresh_preview).pack(
            side=tk.LEFT, padx=(0, 8)
        )
        ttk.Button(actions, text="2. CSV 작업표 만들기", command=self.save_work_csv).pack(
            side=tk.LEFT, padx=(0, 8)
        )
        ttk.Button(actions, text="3. AI 장면표 매핑", command=self.import_scene_table).pack(
            side=tk.LEFT, padx=(0, 8)
        )
        self.start_button = ttk.Button(
            actions,
            text="4. 최종 영상 생성",
            command=self.start,
            style="Primary.TButton",
        )
        self.start_button.pack(side=tk.LEFT, padx=(4, 0))
        self.stop_button = ttk.Button(
            actions,
            text="중지",
            command=self.stop_render,
            state=tk.DISABLED,
        )
        self.stop_button.pack(side=tk.LEFT, padx=(8, 0))
        ttk.Button(actions, text="로그 지우기", command=self.clear_log).pack(
            side=tk.LEFT, padx=(12, 0)
        )

        guide_panel = ttk.LabelFrame(root, text="사용 안내", padding=(10, 8), style="Panel.TLabelframe")
        guide_panel.pack(fill=tk.X, pady=(0, 10))
        guide_panel.columnconfigure(0, weight=1)
        ttk.Label(
            guide_panel,
            text="사용 순서: SRT/영상 폴더 선택 -> CSV 작업표 만들기 또는 AI 장면표 매핑 -> 미리보기 확인 -> 최종 영상 생성",
            style="Panel.TLabel",
        ).grid(row=0, column=0, sticky=tk.W)
        ttk.Label(
            guide_panel,
            text=effect_help_text() + "  |  효과시간초는 비우면 자동",
            style="Panel.TLabel",
        ).grid(row=1, column=0, sticky=tk.W, pady=(6, 0))
        ttk.Label(
            guide_panel,
            text="자동 모드: 영상이 조금 짧으면 느리게 늘리고, 많이 짧으면 반복 후 자릅니다. 영상 소리는 제거됩니다.",
            style="Muted.TLabel",
        ).grid(row=2, column=0, sticky=tk.W, pady=(6, 0))
        effect_help_button = ttk.Button(
            guide_panel,
            text="효과 도움말",
            command=self.show_effect_help,
        )
        effect_help_button.grid(row=0, column=1, rowspan=3, sticky=tk.E, padx=(12, 0))
        Tooltip(effect_help_button, effect_tooltip_text())

        status_row = ttk.Frame(root, style="App.TFrame")
        status_row.pack(fill=tk.X, pady=(0, 10))
        ttk.Label(status_row, text="현재 상태:", style="Guide.TLabel").pack(side=tk.LEFT)
        ttk.Label(status_row, textvariable=self.status_var, style="Status.TLabel").pack(
            side=tk.LEFT, padx=(8, 0)
        )

        workspace = ttk.Frame(root, style="App.TFrame")
        workspace.pack(fill=tk.BOTH, expand=True)
        workspace.columnconfigure(0, weight=3)
        workspace.columnconfigure(1, weight=2)
        workspace.rowconfigure(0, weight=1)

        preview_frame = ttk.LabelFrame(workspace, text="대사 기준 배치 미리보기", padding=8, style="Panel.TLabelframe")
        preview_frame.grid(row=0, column=0, sticky=tk.NSEW, padx=(0, 8))
        columns = ("index", "duration", "caption", "action", "effect", "video")
        self.preview_tree = ttk.Treeview(
            preview_frame,
            columns=columns,
            show="headings",
            height=11,
        )
        self.preview_tree.heading("index", text="번호")
        self.preview_tree.heading("duration", text="길이")
        self.preview_tree.heading("caption", text="자막 대사")
        self.preview_tree.heading("action", text="작업")
        self.preview_tree.heading("effect", text="효과")
        self.preview_tree.heading("video", text="연결 영상")
        self.preview_tree.column("index", width=56, anchor=tk.CENTER, stretch=False)
        self.preview_tree.column("duration", width=80, anchor=tk.CENTER, stretch=False)
        self.preview_tree.column("caption", width=320, anchor=tk.W)
        self.preview_tree.column("action", width=82, anchor=tk.CENTER, stretch=False)
        self.preview_tree.column("effect", width=120, anchor=tk.CENTER, stretch=False)
        self.preview_tree.column("video", width=240, anchor=tk.W)
        self.preview_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        preview_scroll = ttk.Scrollbar(preview_frame, command=self.preview_tree.yview)
        preview_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.preview_tree.configure(yscrollcommand=preview_scroll.set)

        log_frame = ttk.LabelFrame(workspace, text="작업 로그", padding=8, style="Panel.TLabelframe")
        log_frame.grid(row=0, column=1, sticky=tk.NSEW)
        self.log_text = tk.Text(
            log_frame,
            wrap=tk.WORD,
            height=14,
            bg="#fbfcfe",
            fg="#1f2937",
            relief=tk.FLAT,
            font=("Consolas", 9),
        )
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        log_scroll = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        log_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.configure(yscrollcommand=log_scroll.set)
        if self.ffmpeg_var.get():
            self.log(f"FFmpeg 자동 설정: {self.ffmpeg_var.get()}")

    def add_path_row(self, parent, row: int, label: str, var: tk.StringVar, command) -> None:
        ttk.Label(parent, text=label, style="Panel.TLabel").grid(
            row=row, column=0, sticky=tk.W, padx=(0, 18), pady=6
        )
        ttk.Entry(parent, textvariable=var).grid(
            row=row, column=1, sticky=tk.EW, padx=(0, 8), pady=6
        )
        ttk.Button(parent, text="선택", command=command).grid(
            row=row, column=2, sticky=tk.E, pady=6
        )

    def update_mode_option_state(self, *_args) -> None:
        if not hasattr(self, "threshold_spinbox"):
            return
        state = tk.NORMAL if self.mode_var.get() == "자동" else tk.DISABLED
        self.threshold_spinbox.configure(state=state)
        self.threshold_hint.configure(state=state)

    def show_effect_help(self) -> None:
        popup = tk.Toplevel(self)
        popup.title("이미지 효과 도움말")
        popup.configure(bg="#ffffff")
        popup.transient(self)
        popup.resizable(False, False)

        body = tk.Frame(popup, bg="#ffffff", padx=18, pady=16)
        body.pack(fill=tk.BOTH, expand=True)
        tk.Label(
            body,
            text="이미지 효과 도움말",
            bg="#ffffff",
            fg="#182235",
            font=("Malgun Gothic", 12, "bold"),
        ).pack(anchor=tk.W, pady=(0, 10))
        tk.Label(
            body,
            text=effect_help_detail(),
            justify=tk.LEFT,
            bg="#ffffff",
            fg="#243043",
            font=("Malgun Gothic", 11),
        ).pack(anchor=tk.W)
        ttk.Button(body, text="확인", command=popup.destroy).pack(anchor=tk.E, pady=(14, 0))

        popup.update_idletasks()
        x = self.winfo_rootx() + max(0, (self.winfo_width() - popup.winfo_width()) // 2)
        y = self.winfo_rooty() + max(0, (self.winfo_height() - popup.winfo_height()) // 2)
        popup.geometry(f"+{x}+{y}")
        popup.grab_set()

    def browse_srt(self) -> None:
        path = filedialog.askopenfilename(
            title="SRT 파일 선택",
            filetypes=[("SRT files", "*.srt"), ("All files", "*.*")],
        )
        if path:
            self.srt_var.set(path)
            if not self.output_var.get():
                self.output_var.set(str(Path(path).with_name("subtitle_clip_result.mp4")))
            self.refresh_preview()

    def browse_video_dir(self) -> None:
        path = filedialog.askdirectory(title="영상 폴더 선택")
        if path:
            self.video_dir_var.set(path)
            self.refresh_preview()

    def browse_output(self) -> None:
        path = filedialog.asksaveasfilename(
            title="결과 MP4 저장 위치/파일명 선택",
            defaultextension=".mp4",
            filetypes=[("MP4 video", "*.mp4")],
        )
        if path:
            self.output_var.set(path)

    def browse_csv(self) -> None:
        path = filedialog.askopenfilename(
            title="CSV 매핑 파일 선택",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
        )
        if path:
            self.csv_var.set(path)
            self.refresh_preview()

    def browse_ffmpeg(self) -> None:
        path = filedialog.askopenfilename(
            title="ffmpeg.exe 선택",
            filetypes=[("ffmpeg.exe", "ffmpeg.exe"), ("Executable", "*.exe"), ("All files", "*.*")],
        )
        if path:
            self.ffmpeg_var.set(path)

    def get_work_range(self) -> tuple[int, int | None]:
        try:
            start_index = int(self.range_start_var.get())
        except (tk.TclError, ValueError) as exc:
            raise ValueError("작업 시작 번호는 숫자여야 합니다.") from exc
        raw_end = self.range_end_var.get().strip()
        try:
            end_index = int(raw_end) if raw_end else None
        except ValueError as exc:
            raise ValueError("작업 종료 번호는 비워두거나 숫자로 입력해야 합니다.") from exc
        if start_index < 1:
            raise ValueError("작업 시작 번호는 1 이상이어야 합니다.")
        if end_index is not None and end_index < start_index:
            raise ValueError("작업 종료 번호는 시작 번호보다 크거나 같아야 합니다.")
        return start_index, end_index

    def validate(self) -> tuple[Path, Path, Path]:
        srt = Path(self.srt_var.get().strip())
        videos = Path(self.video_dir_var.get().strip())
        output = Path(self.output_var.get().strip())
        if not srt.exists():
            raise ValueError("SRT 파일을 선택하세요.")
        if not videos.exists() or not videos.is_dir():
            raise ValueError("영상 폴더를 선택하세요.")
        if not output.name.lower().endswith(".mp4"):
            raise ValueError("저장 위치는 .mp4 파일이어야 합니다.")
        if self.threshold_var.get() <= 1:
            raise ValueError("자동 느리게 기준은 1보다 커야 합니다.")
        if self.last_duration_var.get() <= 0:
            raise ValueError("마지막 자막 길이는 0보다 커야 합니다.")
        if self.video_start_number_var.get() < 1:
            raise ValueError("영상 시작 번호는 1 이상이어야 합니다.")
        self.get_work_range()
        return srt, videos, output

    def refresh_preview(self) -> None:
        for item in self.preview_tree.get_children():
            self.preview_tree.delete(item)

        srt_text = self.srt_var.get().strip()
        video_dir_text = self.video_dir_var.get().strip()
        if not srt_text or not video_dir_text:
            return

        srt = Path(srt_text)
        video_dir = Path(video_dir_text)
        if not srt.exists() or not video_dir.exists():
            return

        try:
            captions = read_srt_captions(srt)
            start_index, end_index = self.get_work_range()
            slots = build_slots(captions, float(self.last_duration_var.get()), start_index, end_index)
            videos = discover_videos(video_dir)
            csv_path = Path(self.csv_var.get().strip()) if self.csv_var.get().strip() else None
            placements = build_placements(slots, videos, video_dir, csv_path)
        except Exception as exc:
            self.log(f"미리보기 오류: {exc}")
            return

        for placement in placements:
            slot = placement.slot
            caption = slot.text
            if len(caption) > 90:
                caption = caption[:87] + "..."
            self.preview_tree.insert(
                "",
                tk.END,
                values=(
                    slot.index,
                    f"{slot.duration:.2f}s",
                    caption,
                    display_action(placement),
                    display_effect(placement.effect, placement.effect_duration),
                    placement.video.name if placement.video else "",
                ),
            )

    def save_work_csv(self) -> None:
        srt_text = self.srt_var.get().strip()
        video_dir_text = self.video_dir_var.get().strip()
        if not srt_text:
            messagebox.showerror("입력 확인", "먼저 SRT 파일을 선택하세요.")
            return
        srt = Path(srt_text)
        video_dir = Path(video_dir_text) if video_dir_text else Path(".")
        if not srt.exists():
            messagebox.showerror("입력 확인", "SRT 파일을 찾을 수 없습니다.")
            return

        default_name = srt.with_suffix(".work.csv").name
        path = filedialog.asksaveasfilename(
            title="CSV 작업표 저장",
            initialfile=default_name,
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv")],
        )
        if not path:
            return

        try:
            captions = read_srt_captions(srt)
            start_index, end_index = self.get_work_range()
            slots = build_slots(captions, float(self.last_duration_var.get()), start_index, end_index)
            videos = discover_videos(video_dir) if video_dir.exists() else []
            csv_path = Path(self.csv_var.get().strip()) if self.csv_var.get().strip() else None
            placements = build_placements(slots, videos, video_dir, csv_path)
            write_work_csv(Path(path), placements)
            self.csv_var.set(path)
            self.refresh_preview()
            self.log(effect_help_text())
            messagebox.showinfo("완료", "CSV 작업표를 저장했습니다.")
        except Exception as exc:
            messagebox.showerror("CSV 저장 실패", str(exc))

    def import_scene_table(self) -> None:
        srt_text = self.srt_var.get().strip()
        video_dir_text = self.video_dir_var.get().strip()
        if not srt_text:
            messagebox.showerror("입력 확인", "먼저 SRT 파일을 선택하세요.")
            return
        if not video_dir_text:
            messagebox.showerror("입력 확인", "먼저 영상 폴더를 선택하세요.")
            return

        srt = Path(srt_text)
        video_dir = Path(video_dir_text)
        if not srt.exists():
            messagebox.showerror("입력 확인", "SRT 파일을 찾을 수 없습니다.")
            return
        if not video_dir.exists() or not video_dir.is_dir():
            messagebox.showerror("입력 확인", "영상 폴더를 찾을 수 없습니다.")
            return

        scene_table_path = filedialog.askopenfilename(
            title="AI 장면표 선택",
            filetypes=[("CSV/TSV files", "*.csv *.tsv *.txt"), ("All files", "*.*")],
        )
        if not scene_table_path:
            return

        output_path = srt.with_suffix(".ai_mapping.csv")

        try:
            captions = read_srt_captions(srt)
            start_index, end_index = self.get_work_range()
            slots = build_slots(captions, float(self.last_duration_var.get()), start_index, end_index)
            videos = filter_videos_from_start_number(
                discover_videos(video_dir),
                int(self.video_start_number_var.get()),
            )
            narrations = read_scene_table(Path(scene_table_path))
            placements, missing = build_scene_table_placements(slots, videos, narrations)
            write_work_csv(output_path, placements)
            self.csv_var.set(str(output_path))
            self.refresh_preview()
            self.log("")
            self.log(f"AI 장면표 매핑 CSV 저장: {output_path}")
            self.log(f"영상 시작 번호: {int(self.video_start_number_var.get())}")
            self.log(effect_help_text())
            self.log(f"매칭된 장면: {len(narrations) - len(missing)}개 / {len(narrations)}개")
            if missing:
                self.log("매칭 실패 문구:")
                for narration in missing[:20]:
                    self.log(f"- {narration}")
                if len(missing) > 20:
                    self.log(f"- 외 {len(missing) - 20}개")
                messagebox.showwarning(
                    "일부 매칭 실패",
                    f"AI 장면표 매핑 CSV를 자동 저장했습니다.\n\n"
                    f"{output_path}\n\n"
                    "일부 문구는 자동 매핑되지 않았습니다. Excel에서 CSV를 직접 수정할 수 있습니다.",
                )
            else:
                messagebox.showinfo("완료", f"AI 장면표 매핑 CSV를 자동 저장했습니다.\n\n{output_path}")
        except Exception as exc:
            self.log(f"AI 장면표 매핑 오류: {exc}")
            messagebox.showerror("AI 장면표 매핑 실패", str(exc))

    def set_current_process(self, process: subprocess.Popen | None) -> None:
        with self.process_lock:
            self.current_process = process

    def stop_render(self) -> None:
        if not self.worker or not self.worker.is_alive():
            return
        self.cancel_event.set()
        self.status_var.set("중지 중")
        self.stop_button.configure(state=tk.DISABLED)
        self.log("중지 요청을 보냈습니다.")
        with self.process_lock:
            process = self.current_process
        if process is not None and process.poll() is None:
            try:
                process.terminate()
            except Exception:
                try:
                    process.kill()
                except Exception:
                    pass

    def start(self) -> None:
        if self.worker and self.worker.is_alive():
            messagebox.showinfo("작업 중", "이미 생성 작업이 진행 중입니다.")
            return
        try:
            srt, videos, output = self.validate()
            start_index, end_index = self.get_work_range()
        except Exception as exc:
            messagebox.showerror("입력 확인", str(exc))
            return

        self.cancel_event.clear()
        self.set_current_process(None)
        self.start_button.configure(state=tk.DISABLED)
        self.stop_button.configure(state=tk.NORMAL)
        self.status_var.set("영상 생성 중")
        self.last_error = None
        self.log("")
        self.log("작업을 시작합니다.")
        self.refresh_preview()

        def target() -> None:
            try:
                build_video(
                    srt_path=srt,
                    video_folder=videos,
                    output_path=output,
                    csv_path=Path(self.csv_var.get().strip()) if self.csv_var.get().strip() else None,
                    ffmpeg_text=self.ffmpeg_var.get(),
                    aspect=self.aspect_var.get(),
                    mode=self.mode_var.get(),
                    threshold=float(self.threshold_var.get()),
                    last_duration=float(self.last_duration_var.get()),
                    start_index=start_index,
                    end_index=end_index,
                    keep_temp=bool(self.keep_temp_var.get()),
                    log=self.log_queue.put,
                    cancel_event=self.cancel_event,
                    set_current_process=self.set_current_process,
                )
                self.log_queue.put("__DONE__")
            except RenderCancelled as exc:
                self.log_queue.put(str(exc))
                self.log_queue.put("__CANCELLED__")
            except Exception as exc:
                self.log_queue.put(f"오류: {exc}")
                self.log_queue.put("__FAILED__")

        self.worker = threading.Thread(target=target, daemon=True)
        self.worker.start()

    def log(self, message: str) -> None:
        self.log_text.insert(tk.END, message + "\n")
        self.log_text.see(tk.END)

    def clear_log(self) -> None:
        self.log_text.delete("1.0", tk.END)

    def drain_log_queue(self) -> None:
        try:
            while True:
                message = self.log_queue.get_nowait()
                if message == "__DONE__":
                    self.status_var.set("완료")
                    self.start_button.configure(state=tk.NORMAL)
                    self.stop_button.configure(state=tk.DISABLED)
                    self.set_current_process(None)
                    messagebox.showinfo("완료", "영상 생성이 완료되었습니다.")
                elif message == "__CANCELLED__":
                    self.status_var.set("중지됨")
                    self.start_button.configure(state=tk.NORMAL)
                    self.stop_button.configure(state=tk.DISABLED)
                    self.set_current_process(None)
                elif message == "__FAILED__":
                    self.status_var.set("실패")
                    self.start_button.configure(state=tk.NORMAL)
                    self.stop_button.configure(state=tk.DISABLED)
                    self.set_current_process(None)
                    detail = f"\n\n마지막 오류:\n{self.last_error}" if self.last_error else ""
                    messagebox.showerror("실패", f"영상 생성에 실패했습니다. 작업 로그를 확인하세요.{detail}")
                else:
                    if message.startswith("오류:"):
                        self.last_error = message
                    self.log(message)
        except queue.Empty:
            pass
        self.after(100, self.drain_log_queue)


if __name__ == "__main__":
    App().mainloop()
