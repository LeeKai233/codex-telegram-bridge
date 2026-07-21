from __future__ import annotations

import asyncio
import json
import os
import re
import shlex
import shutil
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

from .codex import CodexClient
from .files import FileCandidate, PathPolicy, PathPolicyError

OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "paths": {"type": "array", "items": {"type": "string"}, "maxItems": 8},
        "explanation": {"type": "string"},
    },
    "required": ["paths", "explanation"],
    "additionalProperties": False,
}

_FD_SEARCH_TIMEOUT_SECONDS = 30.0
_COMMON_EXTENSIONS = frozenset(
    {
        "7z",
        "apk",
        "avi",
        "bmp",
        "bz2",
        "c",
        "cc",
        "cpp",
        "css",
        "csv",
        "doc",
        "docx",
        "epub",
        "flac",
        "gif",
        "go",
        "gz",
        "h",
        "hpp",
        "html",
        "ini",
        "ipynb",
        "java",
        "jpeg",
        "jpg",
        "js",
        "json",
        "jsonl",
        "jsx",
        "log",
        "m4a",
        "md",
        "mkv",
        "mov",
        "mp3",
        "mp4",
        "odt",
        "ogg",
        "pdf",
        "php",
        "png",
        "ppt",
        "pptx",
        "ps1",
        "py",
        "rar",
        "rb",
        "rs",
        "rtf",
        "sh",
        "sql",
        "svg",
        "tar",
        "tex",
        "tgz",
        "toml",
        "ts",
        "tsx",
        "txt",
        "wav",
        "webm",
        "webp",
        "xls",
        "xlsx",
        "xml",
        "yaml",
        "yml",
        "zip",
    }
)
_EXTENSION_RE = re.compile(r"[^\s/\\*?]+\Z")


def _fd_fallback_directories() -> tuple[Path, ...]:
    home = Path.home()
    return (
        home / ".local" / "bin",
        home / ".linuxbrew" / "bin",
        Path("/home/linuxbrew/.linuxbrew/bin"),
        Path("/opt/homebrew/bin"),
        Path("/usr/local/bin"),
        Path("/usr/bin"),
        Path("/bin"),
    )


def _tokens(value: str) -> list[str]:
    normalized = value.casefold().replace("pythonproject", "pythonprojects")
    return re.findall(r"[\w\u4e00-\u9fff]+", normalized)


@dataclass(frozen=True, slots=True)
class FileSearchQuery:
    extensions: tuple[str, ...]
    fragments: tuple[str, ...]


def _explicit_extension(value: str) -> tuple[bool, str | None]:
    folded = value.casefold()
    if folded.startswith("ext:"):
        return True, value[4:]
    if value.startswith("*."):
        return True, value[2:]
    if value.startswith(".") and "/" not in value and "\\" not in value:
        return True, value[1:]
    if folded in _COMMON_EXTENSIONS:
        return False, value
    return False, None


def _normalize_extension(value: str) -> str:
    normalized = value.strip().lstrip(".").casefold()
    if not normalized or _EXTENSION_RE.fullmatch(normalized) is None:
        raise ValueError(f"无效的文件扩展名：{value}")
    return normalized


def parse_file_query(description: str) -> FileSearchQuery:
    try:
        tokens = shlex.split(description)
    except ValueError as exc:
        raise ValueError("文件搜索条件的引号不完整") from exc
    if not tokens:
        raise ValueError("文件搜索条件不能为空")

    extensions: list[str] = []
    fragments: list[str] = []
    for token in tokens:
        explicit, value = _explicit_extension(token)
        if value is not None:
            extension = _normalize_extension(value)
            if extension not in extensions:
                extensions.append(extension)
            continue
        if explicit:
            raise ValueError(f"无效的文件扩展名：{token}")
        fragment = token.replace("\\", "/")
        if fragment.startswith("./"):
            fragment = fragment[2:]
        if fragment:
            fragments.append(fragment)
    if not extensions and not fragments:
        raise ValueError("文件搜索条件不能为空")
    return FileSearchQuery(tuple(extensions), tuple(fragments))


class DirectoryIndex:
    def __init__(self, root: Path, max_depth: int = 5) -> None:
        self.root = root.resolve()
        self.max_depth = max_depth
        self._paths: list[Path] = []

    async def refresh(self) -> None:
        self._paths = await asyncio.to_thread(self._scan)

    def _scan(self) -> list[Path]:
        excluded = {
            ".git",
            ".cache",
            ".local",
            ".npm",
            ".nvm",
            ".pyenv",
            ".venv",
            "node_modules",
            "__pycache__",
        }
        paths = [self.root]
        for current, directories, _ in os.walk(self.root):
            base = Path(current)
            depth = len(base.relative_to(self.root).parts)
            directories[:] = [
                name for name in directories if name not in excluded and not name.startswith(".")
            ]
            if depth >= self.max_depth:
                directories[:] = []
                continue
            paths.extend(base / name for name in directories)
        return paths

    def candidates(self, description: str, limit: int = 8) -> list[Path]:
        literal = Path(description).expanduser()
        if literal.is_absolute() or description.startswith(("~", ".")):
            try:
                resolved = literal.resolve(strict=True)
                if resolved.is_dir() and (resolved == self.root or self.root in resolved.parents):
                    return [resolved]
            except OSError:
                pass
        query_tokens = _tokens(description)
        if not query_tokens:
            return []
        ranked: list[tuple[float, Path]] = []
        query = "/".join(query_tokens)
        for path in self._paths:
            relative = str(path.relative_to(self.root)).casefold()
            haystack = relative.replace("pythonprojects", "pythonproject pythonprojects")
            token_score = sum(token in haystack for token in query_tokens) / len(query_tokens)
            similarity = SequenceMatcher(None, query, relative).ratio()
            basename = path.name.casefold()
            exact_bonus = 0.35 if any(token == basename for token in query_tokens) else 0.0
            score = token_score * 0.7 + similarity * 0.3 + exact_bonus
            if score >= 0.35:
                ranked.append((score, path))
        ranked.sort(key=lambda item: (-item[0], len(item[1].parts), str(item[1])))
        return [path for _, path in ranked[:limit]]


class CodexResolver:
    def __init__(self, client: CodexClient, policy: PathPolicy, directory_index: DirectoryIndex) -> None:
        self.client = client
        self.policy = policy
        self.directory_index = directory_index

    async def resolve_directory(self, description: str) -> list[Path]:
        deterministic = self.directory_index.candidates(description)
        if deterministic:
            return [self.policy.validate_directory(path) for path in deterministic]
        prompt = (
            f"Resolve this natural-language directory description under {self.policy.root}: "
            f"{description!r}. Use read-only shell inspection. Return only real directory paths, "
            "ranked best first, in the required JSON schema. Never return paths outside the allowed root."
        )
        paths = await self._run_resolver_turn(None, self.policy.root, prompt)
        return self._validate_directories(paths)

    async def resolve_files(
        self,
        cwd: Path,
        description: str,
    ) -> list[FileCandidate]:
        query = parse_file_query(description)
        search_root = self.policy.validate_directory(cwd)
        paths = await self._run_fd(search_root, query)
        return await asyncio.to_thread(self._validate_file_results, search_root, query, paths)

    @staticmethod
    def _fd_binary() -> str:
        for name in ("fd", "fdfind"):
            executable = shutil.which(name)
            if executable:
                return executable
        for directory in _fd_fallback_directories():
            for name in ("fd", "fdfind"):
                candidate = directory / name
                if candidate.is_file() and os.access(candidate, os.X_OK):
                    return str(candidate)
        raise OSError("未找到 fd/fdfind（已检查 PATH 和常见安装路径），请先安装 fd-find")

    async def _run_fd(self, cwd: Path, query: FileSearchQuery) -> list[bytes]:
        command = [
            self._fd_binary(),
            "--type",
            "file",
            "--hidden",
            "--no-ignore",
            "--absolute-path",
            "--print0",
            "--color=never",
            "--ignore-case",
        ]
        for extension in query.extensions:
            command.extend(("--extension", extension))
        if query.fragments:
            seed = max(query.fragments, key=lambda value: len(value.encode("utf-8")))
            command.extend(("--full-path", "--fixed-strings", "--", seed))

        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                cwd=str(cwd),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except OSError as exc:
            raise OSError(f"无法启动 fd 文件搜索：{exc}") from exc
        try:
            stdout, stderr = await asyncio.wait_for(
                process.communicate(), timeout=_FD_SEARCH_TIMEOUT_SECONDS
            )
        except TimeoutError as exc:
            if process.returncode is None:
                process.kill()
            await process.communicate()
            raise TimeoutError("fd 文件搜索超时，请缩小搜索条件后重试") from exc

        if process.returncode == 1:
            return []
        if process.returncode != 0:
            detail = os.fsdecode(stderr).strip() or f"退出码 {process.returncode}"
            raise OSError(f"fd 文件搜索失败：{detail[:240]}")
        return [value for value in stdout.split(b"\0") if value]

    def _validate_file_results(
        self, cwd: Path, query: FileSearchQuery, paths: list[bytes]
    ) -> list[FileCandidate]:
        candidates: list[FileCandidate] = []
        seen: set[Path] = set()
        for raw_path in paths:
            try:
                candidate = Path(os.fsdecode(raw_path)).expanduser()
                if not candidate.is_absolute():
                    candidate = cwd / candidate
                relative = candidate.relative_to(cwd).as_posix().casefold()
                if not all(fragment.casefold() in relative for fragment in query.fragments):
                    continue
                if query.extensions and not any(
                    candidate.name.casefold().endswith(f".{extension}")
                    for extension in query.extensions
                ):
                    continue
                validated = self.policy.validate_file(candidate)
            except (OSError, PathPolicyError, ValueError):
                continue
            if validated.path in seen:
                continue
            seen.add(validated.path)
            candidates.append(validated)
        def sort_key(item: FileCandidate) -> tuple[str, str]:
            try:
                relative = item.path.relative_to(cwd)
            except ValueError:
                relative = item.path
            return str(relative).casefold(), str(relative)

        candidates.sort(key=sort_key)
        return candidates

    async def _run_resolver_turn(
        self,
        base_thread_id: str | None,
        cwd: Path,
        prompt: str,
        timeout: int = 120,
        *,
        model: str | None = None,
        effort: str | None = None,
    ) -> list[str]:
        try:
            answer = await self.client.run_ephemeral_turn(
                cwd,
                prompt,
                base_thread_id=base_thread_id,
                output_schema=OUTPUT_SCHEMA,
                timeout=timeout,
                model=model,
                effort=effort,
            )
        except TimeoutError as exc:
            raise TimeoutError("Codex resolver timed out") from exc
        parsed = json.loads(answer)
        return [str(path) for path in parsed.get("paths") or []]

    def _validate_directories(self, paths: list[str]) -> list[Path]:
        result: list[Path] = []
        for value in paths:
            try:
                candidate = self.policy.validate_directory(value)
            except OSError, PathPolicyError:
                continue
            if candidate not in result:
                result.append(candidate)
        return result[:8]
