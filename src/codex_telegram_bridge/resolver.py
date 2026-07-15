from __future__ import annotations

import asyncio
import json
import os
import re
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

from .codex import CodexClient, text_input
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


def _tokens(value: str) -> list[str]:
    normalized = value.casefold().replace("pythonproject", "pythonprojects")
    return re.findall(r"[\w\u4e00-\u9fff]+", normalized)


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
        thread = await self.client.start_thread(self.policy.root, ephemeral=True, read_only=True)
        thread_id = str(thread["id"])
        prompt = (
            f"Resolve this natural-language directory description under {self.policy.root}: "
            f"{description!r}. Use read-only shell inspection. Return only real directory paths, "
            "ranked best first, in the required JSON schema. Never return paths outside the allowed root."
        )
        try:
            paths = await self._run_resolver_turn(thread_id, prompt)
            return self._validate_directories(paths)
        finally:
            await self.client.delete_thread(thread_id)

    async def resolve_files(self, thread_id: str, cwd: Path, description: str) -> list[FileCandidate]:
        fork = await self.client.fork_thread(thread_id, cwd)
        fork_id = str(fork["id"])
        prompt = (
            f"Find local files matching this request: {description!r}. Use the conversation context "
            f"and read-only filesystem inspection. Search the session cwd {cwd} first. "
            f"Search elsewhere under {self.policy.root} only when context strongly indicates it. "
            "Return up to 8 absolute regular-file paths in the required JSON schema. "
            "Do not return credentials, keys, tokens, .env files, or directories."
        )
        try:
            paths = await self._run_resolver_turn(fork_id, prompt, timeout=180)
            candidates: list[FileCandidate] = []
            for value in paths:
                try:
                    candidates.append(self.policy.validate_file(value))
                except OSError, PathPolicyError:
                    continue
            return candidates
        finally:
            await self.client.delete_thread(fork_id)

    async def _run_resolver_turn(self, thread_id: str, prompt: str, timeout: int = 120) -> list[str]:
        turn = await self.client.start_turn(
            thread_id,
            [text_input(prompt)],
            client_message_id=f"resolver-{thread_id}",
            output_schema=OUTPUT_SCHEMA,
        )
        turn_id = str(turn["id"])
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            snapshot = await self.client.read_thread(thread_id, include_turns=True)
            for value in snapshot.get("turns") or []:
                if not isinstance(value, dict) or str(value.get("id")) != turn_id:
                    continue
                if value.get("status") == "failed":
                    raise RuntimeError(str((value.get("error") or {}).get("message") or "Resolver failed"))
                if value.get("status") != "completed":
                    break
                messages = [
                    item.get("text")
                    for item in value.get("items") or []
                    if isinstance(item, dict) and item.get("type") == "agentMessage" and item.get("text")
                ]
                if not messages:
                    return []
                parsed = json.loads(str(messages[-1]))
                return [str(path) for path in parsed.get("paths") or []]
            await asyncio.sleep(1)
        raise TimeoutError("Codex resolver timed out")

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
