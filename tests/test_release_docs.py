from __future__ import annotations

import hashlib
import os
import re
import subprocess
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
README = ROOT / "README.md"
INSTALLER = ROOT / "install.sh"
PYPROJECT = ROOT / "pyproject.toml"

EXPECTED_SDIST_INCLUDES = [
    "/src",
    "/tests",
    "/systemd",
    "/LICENSE",
    "/README.md",
    "/RELEASING.md",
    "/install.sh",
    "/pyproject.toml",
    "/uv.lock",
]


def bootstrap_command() -> str:
    return next(
        line
        for line in README.read_text(encoding="utf-8").splitlines()
        if line.startswith("bash -c '") and "releases/download/v0.1.0/install.sh" in line
    )


def test_readme_bootstrap_checksum_matches_release_installer() -> None:
    readme = README.read_text(encoding="utf-8")
    command = bootstrap_command()
    match = re.search(r'sha256="([0-9a-f]{64})"', command)

    assert match is not None
    assert match.group(1) == hashlib.sha256(INSTALLER.read_bytes()).hexdigest()
    assert "bash <(" not in readme
    assert 'curl --proto "=https" --tlsv1.2' in command
    assert ' -o "$installer" "$url"' in command
    assert 'sha256sum -c -' in command
    assert command.endswith('bash "$installer"\'')


def test_readme_bootstrap_propagates_download_failure_and_cleans_temp_file(
    tmp_path: Path,
) -> None:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    fake_curl = fake_bin / "curl"
    fake_curl.write_text("#!/bin/sh\nexit 22\n", encoding="utf-8")
    fake_curl.chmod(0o700)
    temp_dir = tmp_path / "tmp"
    temp_dir.mkdir()
    env = os.environ.copy()
    env.update({"PATH": f"{fake_bin}:/usr/bin:/bin", "TMPDIR": str(temp_dir)})

    result = subprocess.run(
        ["bash", "-c", bootstrap_command()],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 22
    assert list(temp_dir.iterdir()) == []


def test_sdist_uses_an_explicit_public_file_allowlist() -> None:
    document = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))
    includes = document["tool"]["hatch"]["build"]["targets"]["sdist"]["include"]

    assert includes == EXPECTED_SDIST_INCLUDES
    assert all("assets" not in entry and ".codegraph" not in entry for entry in includes)
