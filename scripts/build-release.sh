#!/usr/bin/env bash
# Build the release artifacts: the server (which `workspace add` installs on
# remote hosts, described by release-manifest.json) plus the two binaries a user
# runs locally, so getting started needs no Rust toolchain. Used by CI
# (.github/workflows/release.yml) and locally to produce a testable release base:
#
#   scripts/build-release.sh                 # -> dist/ for the host musl target
#   scripts/build-release.sh <target> <dir>  # explicit target / output dir
#
# Everything is statically linked against musl, so one download runs on any
# Linux of that architecture regardless of glibc version. The version fields are
# read from the built binary's --version-json, so the manifest can never
# disagree with the artifact it describes; that requires the artifact to run on
# the build host (true for x86_64 host + x86_64 musl target).
set -euo pipefail

TARGET="${1:-x86_64-unknown-linux-musl}"
OUT_DIR="${2:-dist}"

case "$TARGET" in
  x86_64-unknown-linux-musl)  OS=linux; ARCH=x86_64  ;;
  aarch64-unknown-linux-musl) OS=linux; ARCH=aarch64 ;;
  *) echo "unsupported target: $TARGET" >&2; exit 1 ;;
esac
SUFFIX="$OS-$ARCH-musl"
SERVER_FILE="remote-workspace-server-$SUFFIX"

echo "Building remote-workspace for $TARGET" >&2
cargo build --release --target "$TARGET" \
  -p remote-workspace-server -p remote-workspace-client -p remote-workspace-mcp

mkdir -p "$OUT_DIR"
touch "$OUT_DIR/SHA256SUMS"

# Record one checksum line per artifact, replacing any prior line for the same
# file so repeated or multi-arch runs accumulate rather than duplicate.
record() {
  local file="$1" sha
  sha="$(sha256sum "$OUT_DIR/$file" | cut -d' ' -f1)"
  grep -v "  $file\$" "$OUT_DIR/SHA256SUMS" > "$OUT_DIR/SHA256SUMS.tmp" || true
  echo "$sha  $file" >> "$OUT_DIR/SHA256SUMS.tmp"
  mv "$OUT_DIR/SHA256SUMS.tmp" "$OUT_DIR/SHA256SUMS"
  printf '%s' "$sha"
}

for bin in remote-workspace remote-workspace-server remote-workspace-mcp; do
  cp "target/$TARGET/release/$bin" "$OUT_DIR/$bin-$SUFFIX"
  chmod +x "$OUT_DIR/$bin-$SUFFIX"
  record "$bin-$SUFFIX" >/dev/null
  echo "Wrote $OUT_DIR/$bin-$SUFFIX" >&2
done

# The manifest describes only the server: it is the contract `workspace add`
# uses to pick and verify what to install remotely. The client binaries are
# plain release assets for humans.
VERSION_JSON="$("$OUT_DIR/$SERVER_FILE" --version-json)"
SOFTWARE_VERSION="$(printf '%s' "$VERSION_JSON" | jq -r '.software_version')"
PROTOCOL_VERSION="$(printf '%s' "$VERSION_JSON" | jq -r '.protocol_version')"
SERVER_SHA="$(sha256sum "$OUT_DIR/$SERVER_FILE" | cut -d' ' -f1)"

jq -n \
  --arg sv "$SOFTWARE_VERSION" \
  --argjson pv "$PROTOCOL_VERSION" \
  --arg os "$OS" --arg arch "$ARCH" --arg file "$SERVER_FILE" --arg sha "$SERVER_SHA" \
  '{software_version: $sv, protocol_version: $pv,
    artifacts: [{os: $os, arch: $arch, file: $file, sha256: $sha}]}' \
  > "$OUT_DIR/release-manifest.json"

echo "Manifest: software_version=$SOFTWARE_VERSION protocol_version=$PROTOCOL_VERSION" >&2
