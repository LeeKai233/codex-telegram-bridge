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
PACKAGE_INIT = ROOT / "src" / "codex_telegram_bridge" / "__init__.py"
RELEASING = ROOT / "RELEASING.md"
SYSTEMD = ROOT / "systemd"

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
        if line.startswith("bash -c '") and "releases/download/v0.3.0/install.sh" in line
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


def test_release_version_is_consistent_across_public_artifacts() -> None:
    version = tomllib.loads(PYPROJECT.read_text(encoding="utf-8"))["project"]["version"]
    installer = INSTALLER.read_text(encoding="utf-8")
    readme = README.read_text(encoding="utf-8")
    package_init = PACKAGE_INIT.read_text(encoding="utf-8")
    releasing = RELEASING.read_text(encoding="utf-8")
    installer_match = re.search(r'^readonly INSTALLER_VERSION="([^"]+)"$', installer, re.MULTILINE)
    package_match = re.search(r'^__version__ = "([^"]+)"$', package_init, re.MULTILINE)

    assert version == "0.3.0"
    assert installer_match is not None and installer_match.group(1) == version
    assert package_match is not None and package_match.group(1) == version
    assert f"releases/download/v{version}/install.sh" in readme
    assert f"Bridge v{version}" in readme
    assert f"gh release create v{version}" in releasing
    assert f"codex_telegram_bridge-{version}-py3-none-any.whl" in releasing
    assert f"codex_telegram_bridge-{version}.tar.gz" in releasing
    assert "sha256sum -c SHA256SUMS" in releasing
    for unit in SYSTEMD.glob("*.service"):
        assert f"# X-CodexTelegramBridge-Installer-Version: v{version}" in unit.read_text(
            encoding="utf-8"
        )


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
