#!/usr/bin/env bash
# Build the release artifacts: the server (which `workspace add` installs on
# remote hosts, described by release-manifest.json) plus the two binaries a user
# runs locally, so getting started needs no Rust toolchain. Used by CI
# (.github/workflows/release.yml) and locally to produce a testable release base:
#
#   scripts/build-release.sh                 # -> dist/ for Linux x86_64
#   scripts/build-release.sh <target> <dir>  # explicit target / output dir
#
# Linux artifacts are statically linked against musl. Windows releases contain
# only the client and MCP binaries; the install manifest remains Linux-only so
# `workspace add` cannot install an unsupported Windows server.
set -euo pipefail

TARGET="${1:-x86_64-unknown-linux-musl}"
OUT_DIR="${2:-dist}"

case "$TARGET" in
  x86_64-unknown-linux-musl)
    OS=linux; ARCH=x86_64; SUFFIX=linux-x86_64-musl; EXT=""
    BINS=(remote-workspace remote-workspace-server remote-workspace-mcp)
    SUMS_FILE=SHA256SUMS
    ;;
  aarch64-unknown-linux-musl)
    OS=linux; ARCH=aarch64; SUFFIX=linux-aarch64-musl; EXT=""
    BINS=(remote-workspace remote-workspace-server remote-workspace-mcp)
    SUMS_FILE=SHA256SUMS
    ;;
  x86_64-pc-windows-msvc)
    OS=windows; ARCH=x86_64; SUFFIX=windows-x86_64; EXT=.exe
    BINS=(remote-workspace remote-workspace-mcp)
    SUMS_FILE=SHA256SUMS-windows-x86_64
    ;;
  *) echo "unsupported target: $TARGET" >&2; exit 1 ;;
esac

echo "Building remote-workspace for $TARGET" >&2
cargo_args=(--release --target "$TARGET")
for package in "${BINS[@]}"; do
  case "$package" in
    remote-workspace) cargo_args+=(-p remote-workspace-client) ;;
    remote-workspace-server) cargo_args+=(-p remote-workspace-server) ;;
    remote-workspace-mcp) cargo_args+=(-p remote-workspace-mcp) ;;
  esac
done
cargo build "${cargo_args[@]}"

mkdir -p "$OUT_DIR"
touch "$OUT_DIR/$SUMS_FILE"

# Record one checksum line per artifact, replacing any prior line for the same
# file so repeated or multi-arch runs accumulate rather than duplicate.
record() {
  local file="$1" sha
  sha="$(sha256sum "$OUT_DIR/$file" | cut -d' ' -f1)"
  grep -v "  $file\$" "$OUT_DIR/$SUMS_FILE" > "$OUT_DIR/$SUMS_FILE.tmp" || true
  echo "$sha  $file" >> "$OUT_DIR/$SUMS_FILE.tmp"
  mv "$OUT_DIR/$SUMS_FILE.tmp" "$OUT_DIR/$SUMS_FILE"
  printf '%s' "$sha"
}

for bin in "${BINS[@]}"; do
  file="$bin-$SUFFIX$EXT"
  cp "target/$TARGET/release/$bin$EXT" "$OUT_DIR/$file"
  if [[ "$OS" == linux ]]; then
    chmod +x "$OUT_DIR/$file"
  fi
  record "$file" >/dev/null
  echo "Wrote $OUT_DIR/$file" >&2
done

# The manifest describes only the server: it is the contract `workspace add`
# uses to pick and verify what to install remotely. The client binaries are
# plain release assets for humans.
if [[ "$OS" == windows ]]; then
  exit 0
fi

SERVER_FILE="remote-workspace-server-$SUFFIX"
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
