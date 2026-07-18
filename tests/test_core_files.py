from __future__ import annotations

import json
import os
import time
from io import BytesIO
from pathlib import Path

import pytest

from codex_telegram_bridge.files import (
    PathPolicy,
    PathPolicyError,
    cleanup_inbox,
    prepare_inbox_path,
    safe_filename,
    sha256_file,
)
from codex_telegram_bridge.resolver import CodexResolver, DirectoryIndex


@pytest.mark.parametrize(
    "relative",
    [
        ".codex/auth.json",
        ".env",
        ".env.local",
        ".aws/credentials",
        ".azure/accessTokens.json",
        ".config/gcloud/application_default_credentials.json",
        ".config/codex/config.toml",
        ".config/fish/config.fish",
        ".config/rclone/rclone.conf",
        ".local/state/codex-telegram-bridge/state.sqlite3",
        ".ssh/id_ed25519",
        ".terraform.d/credentials.tfrc.json",
        ".cargo/credentials.toml",
        ".bashrc",
        ".bashrc.bak",
        ".bash_history",
        ".git-credentials",
        ".yarnrc.yml",
        "certificate.pem",
    ],
)
def test_sensitive_files_are_rejected(tmp_path: Path, relative: str) -> None:
    path = tmp_path / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("secret", encoding="utf-8")
    policy = PathPolicy(tmp_path, upload_limit=1000)
    with pytest.raises(PathPolicyError, match="敏感"):
        policy.validate_file(path)


@pytest.mark.asyncio
async def test_resolver_relative_file_paths_are_anchored_to_session_cwd(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir()
    report = project / "report.txt"
    report.write_text("report", encoding="utf-8")

    calls: list[dict[str, object]] = []

    class Client:
        async def run_ephemeral_turn(
            self, _cwd: Path, _prompt: str, **kwargs: object
        ) -> str:
            calls.append(kwargs)
            return json.dumps({"paths": ["report.txt"]})

    policy = PathPolicy(tmp_path, upload_limit=1000)
    resolver = CodexResolver(Client(), policy, DirectoryIndex(tmp_path))  # type: ignore[arg-type]

    candidates = await resolver.resolve_files(
        "thread",
        project,
        "the report",
        model="gpt-5.6-luna",
        effort="medium",
    )

    assert [candidate.path for candidate in candidates] == [report.resolve()]
    assert calls[0]["model"] == "gpt-5.6-luna"
    assert calls[0]["effort"] == "medium"


def test_path_outside_root_and_symlink_escape_are_rejected(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    outside = tmp_path / "outside.txt"
    outside.write_text("outside", encoding="utf-8")
    link = root / "link.txt"
    link.symlink_to(outside)
    policy = PathPolicy(root, upload_limit=1000)
    with pytest.raises(PathPolicyError, match="位于"):
        policy.validate_file(outside)
    with pytest.raises(PathPolicyError, match="位于"):
        policy.validate_file(link)


def test_open_outbound_rejects_file_replaced_after_confirmation(tmp_path: Path) -> None:
    path = tmp_path / "report.txt"
    path.write_text("approved", encoding="utf-8")
    policy = PathPolicy(tmp_path, upload_limit=1000)
    candidate = policy.validate_file(path)
    replacement = tmp_path / "replacement"
    replacement.write_text("changed", encoding="utf-8")
    os.replace(replacement, path)
    with pytest.raises(PathPolicyError, match="确认后发生变化"), policy.open_outbound(candidate):
        pass


def test_open_outbound_reads_from_anchored_descriptor(tmp_path: Path) -> None:
    path = tmp_path / "report.txt"
    path.write_bytes(b"approved")
    policy = PathPolicy(tmp_path, upload_limit=1000)
    candidate = policy.validate_file(path)
    with policy.open_outbound(candidate) as handle:
        assert handle.read() == b"approved"
    assert sha256_file(path, policy=policy) == sha256_file(path)


def test_project_directory_creation_is_explicit_anchored_and_private(tmp_path: Path) -> None:
    root = tmp_path / "root"
    root.mkdir()
    policy = PathPolicy(root, upload_limit=1000)
    target = root / "projects" / "new-session"

    assert policy.prepare_directory_creation("relative/project") is None
    assert policy.prepare_directory_creation(target) == target
    assert not target.exists()

    created = policy.create_directory(target)

    assert created == target.resolve()
    assert created.is_dir()
    assert (root / "projects").stat().st_mode & 0o777 == 0o700
    assert created.stat().st_mode & 0o777 == 0o700


def test_project_directory_creation_rejects_escape_and_symlink_ancestor(tmp_path: Path) -> None:
    root = tmp_path / "root"
    outside = tmp_path / "outside"
    root.mkdir()
    outside.mkdir()
    policy = PathPolicy(root, upload_limit=1000)
    link = root / "linked"
    link.symlink_to(outside, target_is_directory=True)

    with pytest.raises(PathPolicyError, match="位于"):
        policy.prepare_directory_creation(outside / "project")
    with pytest.raises(PathPolicyError, match="位于"):
        policy.prepare_directory_creation(link / "project")
    with pytest.raises(PathPolicyError, match="敏感"):
        policy.prepare_directory_creation(root / ".codex" / "project")


def test_project_creation_rejects_final_symlink_swapped_after_confirmation(
    tmp_path: Path,
) -> None:
    root = tmp_path / "root"
    root.mkdir()
    alternate = root / "another-project"
    alternate.mkdir()
    target = root / "confirmed-project"
    policy = PathPolicy(root, upload_limit=1000)

    assert policy.prepare_directory_creation(target) == target
    target.symlink_to(alternate, target_is_directory=True)

    with pytest.raises(PathPolicyError, match="非目录|安全创建"):
        policy.create_directory(target)


def test_inbox_download_uses_part_then_atomic_commit(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    destination = prepare_inbox_path(
        inbox,
        "../../thread/id",
        "../../report.txt",
        expected_size=7,
        download_limit=20,
        quota_bytes=100,
        minimum_free_bytes=0,
    )
    assert destination.part_path.suffix == ".part"
    assert destination.part_path.exists()
    assert not destination.final_path.exists()
    assert destination.part_path.is_relative_to(inbox)
    assert destination.write_from(BytesIO(b"content")) == 7
    result = destination.commit()
    assert result == destination.final_path
    assert result.read_bytes() == b"content"
    assert not destination.part_path.exists()
    assert result.stat().st_mode & 0o777 == 0o600


def test_inbox_commit_removes_incomplete_download(tmp_path: Path) -> None:
    destination = prepare_inbox_path(
        tmp_path / "inbox",
        "thread",
        "file.bin",
        expected_size=10,
        download_limit=20,
        quota_bytes=100,
        minimum_free_bytes=0,
    )
    with pytest.raises(PathPolicyError, match="不完整"):
        destination.write_from(BytesIO(b"short"))
    assert not destination.part_path.exists()


def test_inbox_commit_rejects_part_path_replaced_while_descriptor_is_held(
    tmp_path: Path,
) -> None:
    destination = prepare_inbox_path(
        tmp_path / "inbox",
        "thread",
        "file.bin",
        expected_size=7,
        download_limit=20,
        quota_bytes=100,
        minimum_free_bytes=0,
    )
    destination.write_from(BytesIO(b"content"))
    destination.part_path.unlink()
    destination.part_path.write_bytes(b"attacker")

    with pytest.raises(PathPolicyError, match="发生变化"):
        destination.commit()

    assert not destination.part_path.exists()
    assert not destination.final_path.exists()


def test_safe_filename_respects_utf8_name_max_with_part_suffix(tmp_path: Path) -> None:
    name = safe_filename(f"{'测试' * 200}.txt")
    assert name.endswith(".txt")
    assert len(name.encode("utf-8")) <= 250

    destination = prepare_inbox_path(
        tmp_path / "inbox",
        "thread",
        f"{'测试' * 200}.txt",
        expected_size=0,
        download_limit=20,
        quota_bytes=100,
        minimum_free_bytes=0,
    )
    try:
        assert len(destination.final_path.name.encode("utf-8")) <= 250
        assert len(destination.part_path.name.encode("utf-8")) <= 255
    finally:
        destination.abort()


def test_inbox_limits_are_checked_before_download(tmp_path: Path) -> None:
    with pytest.raises(PathPolicyError, match="超过"):
        prepare_inbox_path(
            tmp_path / "inbox",
            "thread",
            "large.bin",
            expected_size=21,
            download_limit=20,
            quota_bytes=100,
            minimum_free_bytes=0,
        )
    with pytest.raises(PathPolicyError, match="配额"):
        prepare_inbox_path(
            tmp_path / "inbox-2",
            "thread",
            "large.bin",
            expected_size=20,
            download_limit=20,
            quota_bytes=10,
            minimum_free_bytes=0,
        )


def test_cleanup_preserves_queued_paths_and_does_not_follow_symlinks(tmp_path: Path) -> None:
    inbox = tmp_path / "inbox"
    directory = inbox / "thread" / "download"
    directory.mkdir(parents=True)
    protected = directory / "queued.txt"
    removable = directory / "old.txt"
    protected.write_text("keep", encoding="utf-8")
    removable.write_text("remove", encoding="utf-8")
    outside = tmp_path / "outside.txt"
    outside.write_text("outside", encoding="utf-8")
    link = directory / "old-link"
    link.symlink_to(outside)
    old = time.time() - 3 * 86400
    os.utime(protected, (old, old))
    os.utime(removable, (old, old))
    os.utime(link, (old, old), follow_symlinks=False)
    removed = cleanup_inbox(inbox, 1, protected_paths={protected.resolve()})
    assert removed == 2
    assert protected.exists()
    assert not removable.exists()
    assert not link.exists()
    assert outside.read_text(encoding="utf-8") == "outside"
