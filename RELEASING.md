# Release process

`v0.1.0` and later public installers rely on GitHub immutable releases. Do not create or push the
release tag before release immutability is enabled.

## Release candidate

1. Keep `pyproject.toml`, `install.sh`, README URLs, unit metadata, and release notes on the same
   version.
2. Run the complete local gate:

   ```bash
   uv lock --check
   uv run --frozen ruff check .
   uv run --frozen pytest
   uv build
   bash -n install.sh
   uvx --from shellcheck-py shellcheck install.sh
   systemd-analyze --user verify systemd/*.service
   git diff --check
   ```

3. Confirm `tests/test_release_docs.py` matches the README bootstrap SHA-256 to the exact
   `install.sh` bytes.
4. Commit, push `main`, and wait for both GitHub Actions jobs to pass on the exact release commit.

## Immutable publication

1. Before the repository's first release, open **Settings**, scroll to **Releases**, and select
   **Enable release immutability**. The setting only applies to releases created afterward.
2. Build assets from the CI-passed commit and prepare checksums outside the checkout:

   ```bash
   release_dir="$(mktemp -d)"
   cp install.sh dist/codex_telegram_bridge-0.3.0-py3-none-any.whl \
      dist/codex_telegram_bridge-0.3.0.tar.gz "$release_dir/"
   (cd "$release_dir" && sha256sum install.sh *.whl *.tar.gz >SHA256SUMS)
   ```

3. Create a draft targeting the exact commit. Creating the release creates the tag; do not push a
   separate movable tag first.

   ```bash
   release_sha="$(git rev-parse HEAD)"
   gh release create v0.3.0 --repo LeeKai233/codex-telegram-bridge \
      --draft --target "$release_sha" --title "v0.3.0" \
      "$release_dir/install.sh" "$release_dir/SHA256SUMS" \
      "$release_dir/codex_telegram_bridge-0.3.0-py3-none-any.whl" \
      "$release_dir/codex_telegram_bridge-0.3.0.tar.gz"
   ```

4. Review the draft assets and notes, then publish once. Confirm the release is marked immutable,
   the remote tag equals the CI-passed commit, and GitHub reports an asset digest.
5. For a private bootstrap repository, make it public only after the immutable release exists.

## Public smoke test

From a temporary directory with no project checkout, download and verify every public release
asset before testing the pinned tag:

```bash
release_dir="$(mktemp -d)"
cd "$release_dir"
base="https://github.com/LeeKai233/codex-telegram-bridge/releases/download/v0.3.0"
for asset in install.sh SHA256SUMS \
  codex_telegram_bridge-0.3.0-py3-none-any.whl \
  codex_telegram_bridge-0.3.0.tar.gz; do
  curl --proto "=https" --tlsv1.2 -fL --retry 3 --retry-all-errors \
    -o "$asset" "$base/$asset"
done
sha256sum -c SHA256SUMS
bash install.sh --version
UV_TOOL_DIR="$release_dir/tools" UV_TOOL_BIN_DIR="$release_dir/bin" \
  uv tool install --python 3.14 \
  git+https://github.com/LeeKai233/codex-telegram-bridge@v0.3.0
"$release_dir/bin/codex-tg" --help
"$release_dir/bin/codex-telegram-bridge" --help
```

Run the README bootstrap only on a supported WSL2 host that passes `install.sh --check-only`. Never
bypass the swap/disk gate for a release test.
