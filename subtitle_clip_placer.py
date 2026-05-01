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


VIDEO_EXTENSIONS = {".mp4", ".mov", ".mkv", ".webm", ".avi", ".m4v"}


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


@dataclass
class RenderRun:
    action: str
    video: Path | None
    slots: list[Slot]

    @property
    def duration(self) -> float:
        return sum(slot.duration for slot in self.slots)

    @property
    def start(self) -> float:
        return self.slots[0].start

    @property
    def end(self) -> float:
        return self.slots[-1].end


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


def build_slots(captions: list[Caption], last_duration: float) -> list[Slot]:
    slots: list[Slot] = []
    for index, caption in enumerate(captions):
        start = caption.start
        end = captions[index + 1].start if index + 1 < len(captions) else start + last_duration
        if end <= start:
            continue
        slots.append(Slot(index=index + 1, start=start, end=end, text=caption.text))
    if not slots:
        raise ValueError("유효한 자막 구간을 만들 수 없습니다.")
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
        if item.is_file() and item.suffix.lower() in VIDEO_EXTENSIONS
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
    if raw in {"영상", "새영상", "video", "clip"}:
        return "video"
    if raw in {"이전유지", "유지", "계속", "previous", "prev", "hold"}:
        return "hold"
    if raw in {"검은화면", "빈화면", "없음", "건너뜀", "blank", "black", "none", "skip"}:
        return "blank"
    raise ValueError(f"지원하지 않는 작업 값입니다: {value}")


def action_label(action: str) -> str:
    if action == "video":
        return "영상"
    if action == "hold":
        return "이전유지"
    if action == "blank":
        return "검은화면"
    return action


def read_csv_placement_overrides(csv_path: Path, video_folder: Path) -> dict[int, tuple[str, Path | None]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(file)
        if not reader.fieldnames:
            raise ValueError("CSV 헤더를 찾지 못했습니다.")
        number_key = "번호" if "번호" in reader.fieldnames else None
        video_key = "영상파일" if "영상파일" in reader.fieldnames else None
        action_key = "작업" if "작업" in reader.fieldnames else None
        if number_key is None or video_key is None:
            raise ValueError("CSV에는 '번호'와 '영상파일' 열이 있어야 합니다.")

        mapping: dict[int, tuple[str, Path | None]] = {}
        for row in reader:
            raw_number = (row.get(number_key) or "").strip()
            raw_video = (row.get(video_key) or "").strip()
            raw_action = (row.get(action_key) or "").strip() if action_key else ""
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
                raise ValueError(f"{number}번 대사는 영상파일이 필요합니다.")
            mapping[number] = (action, video)
    return mapping


def build_placements(
    slots: list[Slot],
    videos: list[Path],
    video_folder: Path,
    csv_path: Path | None = None,
) -> list[Placement]:
    slot_count = len(slots)
    matched: list[Path | None]
    try:
        matched = list(match_videos_to_slots(videos, slot_count))
    except Exception:
        video_by_number = {
            number: video
            for video in videos
            if (number := leading_number(video)) is not None
        }
        if video_by_number:
            matched = [video_by_number.get(position) for position in range(1, slot_count + 1)]
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
        for index, (action, video) in mapping.items():
            if 1 <= index <= slot_count:
                placements[index - 1] = Placement(
                    slot=slots[index - 1],
                    action=action,
                    video=video,
                )
    return placements


def write_work_csv(path: Path, placements: list[Placement]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["번호", "시작시간", "길이초", "대사", "작업", "영상파일"],
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
            current = RenderRun(action="video", video=placement.video, slots=[placement.slot])
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


def run_process(command: list[str], log) -> None:
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
    assert process.stdout is not None
    for line in process.stdout:
        line = line.strip()
        if line:
            log(line)
    return_code = process.wait()
    if return_code != 0:
        raise RuntimeError(f"FFmpeg 실행 실패, 종료 코드: {return_code}")


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
        run_process(command, log)
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
        run_process(command, log)
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
    run_process(command, log)
    return f"자르기 ({video_duration:.2f}s -> {slot_duration:.2f}s)"


def render_black_segment(
    ffmpeg: str,
    output: Path,
    duration: float,
    width: int,
    height: int,
    log,
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
    run_process(command, log)
    return f"검은화면 ({duration:.2f}s)"


def concat_segments(ffmpeg: str, segments: list[Path], output: Path, temp_dir: Path, log) -> None:
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
    run_process(command, log)


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
    keep_temp: bool,
    log,
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
    slots = build_slots(captions, last_duration)
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
                )
            else:
                assert run.video is not None
                log(
                    f"[{run_index}/{len(runs)}] {slot_numbers}번 대사 -> {run.video.name} "
                    f"{run.start:.2f}s~{run.end:.2f}s ({run.duration:.2f}s)"
                )
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
                )
            log(f"처리 방식: {result}")
            segments.append(segment)

        log("")
        log("최종 영상을 합치는 중입니다.")
        concat_segments(ffmpeg, segments, output_path, temp_root, log)
        log("")
        log(f"완료: {output_path}")
    finally:
        if keep_temp:
            log(f"임시 파일 보관: {temp_root}")
        else:
            shutil.rmtree(temp_root, ignore_errors=True)


class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Subtitle Clip Placer")
        self.geometry("1040x780")
        self.minsize(900, 660)
        self.configure(bg="#f3f5f7")

        self.log_queue: queue.Queue[str] = queue.Queue()
        self.worker: threading.Thread | None = None

        self.srt_var = tk.StringVar()
        self.video_dir_var = tk.StringVar()
        self.output_var = tk.StringVar()
        self.csv_var = tk.StringVar()
        self.ffmpeg_var = tk.StringVar()
        self.aspect_var = tk.StringVar(value="가로 영상 (1920x1080)")
        self.mode_var = tk.StringVar(value="자동")
        self.threshold_var = tk.DoubleVar(value=1.2)
        self.last_duration_var = tk.DoubleVar(value=6.0)
        self.keep_temp_var = tk.BooleanVar(value=False)

        self.configure_styles()
        self.create_widgets()
        self.after(100, self.drain_log_queue)

    def configure_styles(self) -> None:
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass
        style.configure("App.TFrame", background="#f3f5f7")
        style.configure("Panel.TLabelframe", background="#ffffff", bordercolor="#d8dee7")
        style.configure(
            "Panel.TLabelframe.Label",
            background="#ffffff",
            foreground="#243043",
            font=("Malgun Gothic", 10, "bold"),
        )
        style.configure("App.TLabel", background="#f3f5f7", foreground="#243043")
        style.configure("Panel.TLabel", background="#ffffff", foreground="#243043")
        style.configure("Muted.TLabel", background="#ffffff", foreground="#697386")
        style.configure("TEntry", padding=4)
        style.configure("TCombobox", padding=4)
        style.configure("TButton", padding=(12, 6), font=("Malgun Gothic", 9))
        style.configure(
            "Primary.TButton",
            padding=(16, 7),
            font=("Malgun Gothic", 9, "bold"),
        )
        style.configure("Treeview", rowheight=26, font=("Malgun Gothic", 9))
        style.configure("Treeview.Heading", font=("Malgun Gothic", 9, "bold"))

    def create_widgets(self) -> None:
        root = ttk.Frame(self, padding=18, style="App.TFrame")
        root.pack(fill=tk.BOTH, expand=True)

        header = tk.Frame(root, bg="#182235", height=82)
        header.pack(fill=tk.X, pady=(0, 14))
        header.pack_propagate(False)
        tk.Label(
            header,
            text="Subtitle Clip Placer",
            bg="#182235",
            fg="#ffffff",
            font=("Malgun Gothic", 18, "bold"),
        ).pack(anchor=tk.W, padx=18, pady=(14, 0))
        tk.Label(
            header,
            text="SRT 대사 기준으로 영상 클립을 배치하고 Excel 작업표로 매칭을 관리합니다.",
            bg="#182235",
            fg="#b9c3d4",
            font=("Malgun Gothic", 9),
        ).pack(anchor=tk.W, padx=19, pady=(3, 0))

        form = ttk.LabelFrame(root, text="파일과 출력 설정", padding=12, style="Panel.TLabelframe")
        form.pack(fill=tk.X)
        form.columnconfigure(1, weight=1)

        self.add_path_row(form, 0, "SRT 파일", self.srt_var, self.browse_srt)
        self.add_path_row(form, 1, "영상 폴더", self.video_dir_var, self.browse_video_dir)
        self.add_path_row(form, 2, "저장 위치", self.output_var, self.browse_output)
        self.add_path_row(form, 3, "CSV 매핑(선택)", self.csv_var, self.browse_csv)
        self.add_path_row(form, 4, "ffmpeg.exe", self.ffmpeg_var, self.browse_ffmpeg)

        ttk.Label(form, text="화면 비율", style="Panel.TLabel").grid(row=5, column=0, sticky=tk.W, pady=6)
        aspect = ttk.Combobox(
            form,
            textvariable=self.aspect_var,
            values=["세로 쇼츠 (1080x1920)", "가로 영상 (1920x1080)"],
            state="readonly",
        )
        aspect.grid(row=5, column=1, sticky=tk.EW, pady=6)

        ttk.Label(form, text="부족한 영상 처리", style="Panel.TLabel").grid(row=6, column=0, sticky=tk.W, pady=6)
        mode = ttk.Combobox(
            form,
            textvariable=self.mode_var,
            values=["자동", "반복 후 자르기", "느리게 늘리기"],
            state="readonly",
        )
        mode.grid(row=6, column=1, sticky=tk.EW, pady=6)

        options = ttk.Frame(form)
        options.grid(row=7, column=1, sticky=tk.EW, pady=6)
        ttk.Label(options, text="자동 느리게 기준", style="Panel.TLabel").pack(side=tk.LEFT)
        ttk.Spinbox(
            options,
            from_=1.01,
            to=2.0,
            increment=0.05,
            textvariable=self.threshold_var,
            width=8,
        ).pack(side=tk.LEFT, padx=(8, 18))
        ttk.Label(options, text="마지막 자막 길이(초)", style="Panel.TLabel").pack(side=tk.LEFT)
        ttk.Spinbox(
            options,
            from_=1.0,
            to=30.0,
            increment=0.5,
            textvariable=self.last_duration_var,
            width=8,
        ).pack(side=tk.LEFT, padx=(8, 18))
        ttk.Checkbutton(
            options,
            text="임시 파일 보관",
            variable=self.keep_temp_var,
        ).pack(side=tk.LEFT)

        hint = ttk.Label(
            root,
            text=(
                "자동 모드: 영상이 슬롯보다 조금 짧으면 느리게 늘리고, "
                "많이 짧으면 반복 후 자릅니다. 영상 소리는 제거됩니다."
            ),
            style="App.TLabel",
        )
        hint.pack(fill=tk.X, pady=(10, 10))

        guide = ttk.LabelFrame(root, text="작업 순서", padding=10, style="Panel.TLabelframe")
        guide.pack(fill=tk.X, pady=(0, 12))
        ttk.Label(
            guide,
            text=(
                "1. SRT 파일과 영상 폴더 선택  |  "
                "2. Excel 작업표 만들기  |  "
                "3. Excel에서 작업/영상파일 수정 후 저장  |  "
                "4. CSV 매핑에 수정한 작업표 선택  |  "
                "5. 최종 영상 생성"
            ),
            style="Panel.TLabel",
        ).pack(fill=tk.X, padx=2, pady=(0, 4))
        ttk.Label(
            guide,
            text=(
                "Excel에서는 번호/시작시간/길이초/대사는 건드리지 말고, "
                "'작업'과 '영상파일' 칸만 수정하세요."
            ),
            style="Muted.TLabel",
        ).pack(fill=tk.X, padx=2, pady=(0, 0))

        actions = ttk.Frame(root, style="App.TFrame")
        actions.pack(fill=tk.X, pady=(0, 10))
        ttk.Button(actions, text="1. 대사/영상 확인", command=self.refresh_preview).pack(
            side=tk.LEFT, padx=(0, 8)
        )
        ttk.Button(actions, text="2. Excel 작업표 만들기", command=self.save_work_csv).pack(
            side=tk.LEFT, padx=(0, 8)
        )
        self.start_button = ttk.Button(
            actions,
            text="5. 최종 영상 생성",
            command=self.start,
            style="Primary.TButton",
        )
        self.start_button.pack(side=tk.LEFT)
        ttk.Button(actions, text="로그 지우기", command=self.clear_log).pack(
            side=tk.LEFT, padx=8
        )

        self.progress = ttk.Progressbar(root, mode="indeterminate")
        self.progress.pack(fill=tk.X, pady=(0, 10))

        preview_frame = ttk.LabelFrame(root, text="대사 기준 배치 미리보기", padding=8, style="Panel.TLabelframe")
        preview_frame.pack(fill=tk.BOTH, expand=True, pady=(0, 8))
        columns = ("index", "duration", "caption", "action", "video")
        self.preview_tree = ttk.Treeview(
            preview_frame,
            columns=columns,
            show="headings",
            height=8,
        )
        self.preview_tree.heading("index", text="번호")
        self.preview_tree.heading("duration", text="길이")
        self.preview_tree.heading("caption", text="자막 대사")
        self.preview_tree.heading("action", text="작업")
        self.preview_tree.heading("video", text="연결 영상")
        self.preview_tree.column("index", width=56, anchor=tk.CENTER, stretch=False)
        self.preview_tree.column("duration", width=80, anchor=tk.CENTER, stretch=False)
        self.preview_tree.column("caption", width=360, anchor=tk.W)
        self.preview_tree.column("action", width=82, anchor=tk.CENTER, stretch=False)
        self.preview_tree.column("video", width=260, anchor=tk.W)
        self.preview_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        preview_scroll = ttk.Scrollbar(preview_frame, command=self.preview_tree.yview)
        preview_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.preview_tree.configure(yscrollcommand=preview_scroll.set)

        log_frame = ttk.LabelFrame(root, text="작업 로그", padding=8, style="Panel.TLabelframe")
        log_frame.pack(fill=tk.BOTH, expand=True)
        self.log_text = tk.Text(
            log_frame,
            wrap=tk.WORD,
            height=10,
            bg="#fbfcfe",
            fg="#1f2937",
            relief=tk.FLAT,
            font=("Consolas", 9),
        )
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scroll = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        scroll.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.configure(yscrollcommand=scroll.set)

    def add_path_row(self, parent, row: int, label: str, var: tk.StringVar, command) -> None:
        ttk.Label(parent, text=label, style="Panel.TLabel").grid(row=row, column=0, sticky=tk.W, pady=6)
        ttk.Entry(parent, textvariable=var).grid(
            row=row, column=1, sticky=tk.EW, padx=(0, 8), pady=6
        )
        ttk.Button(parent, text="선택", command=command).grid(
            row=row, column=2, sticky=tk.E, pady=6
        )

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
            title="결과 MP4 저장",
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
            slots = build_slots(captions, float(self.last_duration_var.get()))
            videos = discover_videos(video_dir)
            csv_path = Path(self.csv_var.get().strip()) if self.csv_var.get().strip() else None
            placements = build_placements(slots, videos, video_dir, csv_path)
        except Exception as exc:
            self.log(f"미리보기 오류: {exc}")
            return

        for position, placement in enumerate(placements, start=1):
            slot = placement.slot
            caption = slot.text
            if len(caption) > 90:
                caption = caption[:87] + "..."
            self.preview_tree.insert(
                "",
                tk.END,
                values=(
                    position,
                    f"{slot.duration:.2f}s",
                    caption,
                    action_label(placement.action),
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
            slots = build_slots(captions, float(self.last_duration_var.get()))
            videos = discover_videos(video_dir) if video_dir.exists() else []
            csv_path = Path(self.csv_var.get().strip()) if self.csv_var.get().strip() else None
            placements = build_placements(slots, videos, video_dir, csv_path)
            write_work_csv(Path(path), placements)
            self.csv_var.set(path)
            self.refresh_preview()
            messagebox.showinfo("완료", "CSV 작업표를 저장했습니다.")
        except Exception as exc:
            messagebox.showerror("CSV 저장 실패", str(exc))

    def start(self) -> None:
        if self.worker and self.worker.is_alive():
            messagebox.showinfo("작업 중", "이미 생성 작업이 진행 중입니다.")
            return
        try:
            srt, videos, output = self.validate()
        except Exception as exc:
            messagebox.showerror("입력 확인", str(exc))
            return

        self.start_button.configure(state=tk.DISABLED)
        self.progress.start(10)
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
                    keep_temp=bool(self.keep_temp_var.get()),
                    log=self.log_queue.put,
                )
                self.log_queue.put("__DONE__")
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
                    self.progress.stop()
                    self.start_button.configure(state=tk.NORMAL)
                    messagebox.showinfo("완료", "영상 생성이 완료되었습니다.")
                elif message == "__FAILED__":
                    self.progress.stop()
                    self.start_button.configure(state=tk.NORMAL)
                    messagebox.showerror("실패", "영상 생성에 실패했습니다. 작업 로그를 확인하세요.")
                else:
                    self.log(message)
        except queue.Empty:
            pass
        self.after(100, self.drain_log_queue)


if __name__ == "__main__":
    App().mainloop()
