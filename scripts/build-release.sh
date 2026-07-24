#!/usr/bin/env bash
# Build a static server artifact and emit the release manifest + checksums that
# `agent-remote workspace add` consumes. Used by CI (.github/workflows/release.yml)
# and locally to produce a testable release base:
#
#   scripts/build-release.sh                 # -> dist/ for the host musl target
#   scripts/build-release.sh <target> <dir>  # explicit target / output dir
#
# The version fields are read from the built binary's --version-json, so the
# manifest can never disagree with the artifact it describes. This requires the
# artifact to run on the build host (true for x86_64 host + x86_64 musl target).
set -euo pipefail

TARGET="${1:-x86_64-unknown-linux-musl}"
OUT_DIR="${2:-dist}"

case "$TARGET" in
  x86_64-unknown-linux-musl)  OS=linux; ARCH=x86_64;  FILE=agent-remote-server-linux-x86_64-musl ;;
  aarch64-unknown-linux-musl) OS=linux; ARCH=aarch64; FILE=agent-remote-server-linux-aarch64-musl ;;
  *) echo "unsupported target: $TARGET" >&2; exit 1 ;;
esac

echo "Building agent-remote-server for $TARGET" >&2
cargo build --release --target "$TARGET" -p agent-remote-server

BIN="target/$TARGET/release/agent-remote-server"
mkdir -p "$OUT_DIR"
cp "$BIN" "$OUT_DIR/$FILE"
chmod +x "$OUT_DIR/$FILE"

VERSION_JSON="$("$OUT_DIR/$FILE" --version-json)"
SOFTWARE_VERSION="$(printf '%s' "$VERSION_JSON" | jq -r '.software_version')"
PROTOCOL_VERSION="$(printf '%s' "$VERSION_JSON" | jq -r '.protocol_version')"
SHA256="$(sha256sum "$OUT_DIR/$FILE" | cut -d' ' -f1)"

jq -n \
  --arg sv "$SOFTWARE_VERSION" \
  --argjson pv "$PROTOCOL_VERSION" \
  --arg os "$OS" --arg arch "$ARCH" --arg file "$FILE" --arg sha "$SHA256" \
  '{software_version: $sv, protocol_version: $pv,
    artifacts: [{os: $os, arch: $arch, file: $file, sha256: $sha}]}' \
  > "$OUT_DIR/release-manifest.json"

# Idempotent across re-runs: replace any prior line for this FILE, keep others
# (so a multi-arch run accumulates one line per artifact).
touch "$OUT_DIR/SHA256SUMS"
grep -v "  $FILE\$" "$OUT_DIR/SHA256SUMS" > "$OUT_DIR/SHA256SUMS.tmp" || true
echo "$SHA256  $FILE" >> "$OUT_DIR/SHA256SUMS.tmp"
mv "$OUT_DIR/SHA256SUMS.tmp" "$OUT_DIR/SHA256SUMS"

echo "Wrote $OUT_DIR/$FILE (sha256 $SHA256)" >&2
echo "Manifest: software_version=$SOFTWARE_VERSION protocol_version=$PROTOCOL_VERSION" >&2
