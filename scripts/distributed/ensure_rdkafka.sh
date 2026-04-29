#!/usr/bin/env bash
# ensure_rdkafka.sh — Idempotently build and install librdkafka v2.3.0 from source.
#
# Installs to ~/Desktop/rdkafka_local (or $1 if given).
# No root/sudo required. Works on Ubuntu 22.04 (glibc 2.35) and Ubuntu 24.04+.
# Skips the build if the stamp file already matches the target version.
#
# Usage: ensure_rdkafka.sh [install-dir]
#   Default install-dir: ~/Desktop/rdkafka_local

set -euo pipefail

RDKAFKA_VERSION="v2.3.0"
INSTALL_DIR="${1:-$HOME/Desktop/rdkafka_local}"
STAMP_FILE="$INSTALL_DIR/.ariabc_rdkafka_version"
BUILD_PARENT="/tmp"
TARBALL_URL="https://github.com/confluentinc/librdkafka/archive/refs/tags/${RDKAFKA_VERSION}.tar.gz"
TARBALL="$BUILD_PARENT/librdkafka-${RDKAFKA_VERSION}.tar.gz"
SRC_DIR="$BUILD_PARENT/librdkafka-${RDKAFKA_VERSION#v}"
BUILD_DIR="$BUILD_PARENT/librdkafka-build-${RDKAFKA_VERSION#v}"

log() { echo "[ensure_rdkafka] $*"; }

# Idempotency: skip if already installed at the right version
if [[ -f "$STAMP_FILE" ]]; then
  installed_ver="$(cat "$STAMP_FILE" | tr -d '[:space:]')"
  if [[ "$installed_ver" == "$RDKAFKA_VERSION" && \
        -f "$INSTALL_DIR/lib/librdkafka.so" && \
        -f "$INSTALL_DIR/include/librdkafka/rdkafka.h" ]]; then
    log "librdkafka $RDKAFKA_VERSION already at $INSTALL_DIR — nothing to do"
    exit 0
  fi
fi

log "Building librdkafka $RDKAFKA_VERSION → $INSTALL_DIR"

# Locate cmake (3.16+ required; fall back to portable binary from /tmp)
CMAKE_CMD=""
for cmd in cmake cmake3 /tmp/cmake-3.28.3-linux-x86_64/bin/cmake; do
  if command -v "$cmd" >/dev/null 2>&1 && "$cmd" --version 2>/dev/null | grep -qE 'cmake version [3-9]\.([0-9]{2,}|[2-9][0-9])'; then
    CMAKE_CMD="$cmd"
    break
  fi
done
if [[ -z "$CMAKE_CMD" ]]; then
  # Last try: any cmake that identifies itself
  for cmd in cmake cmake3; do
    if command -v "$cmd" >/dev/null 2>&1; then
      CMAKE_CMD="$cmd"
      break
    fi
  done
fi
if [[ -z "$CMAKE_CMD" ]]; then
  # Last resort: try to install cmake via apt (no-op if already managed, needs sudo)
  if command -v apt-get >/dev/null 2>&1; then
    log "cmake not found — attempting apt-get install cmake..."
    if sudo -n apt-get install -y cmake >/dev/null 2>&1 || \
       (command -v sshpass >/dev/null 2>&1 && false); then
      CMAKE_CMD="$(command -v cmake 2>/dev/null)"
    fi
  fi
fi
if [[ -z "$CMAKE_CMD" ]]; then
  echo "ERROR: cmake not found. Options:" >&2
  echo "  1. sudo apt-get install cmake" >&2
  echo "  2. Extract cmake-3.28.3 to /tmp/ and retry:" >&2
  echo "     wget -P /tmp https://github.com/Kitware/CMake/releases/download/v3.28.3/cmake-3.28.3-linux-x86_64.tar.gz" >&2
  echo "     tar -C /tmp -xzf /tmp/cmake-3.28.3-linux-x86_64.tar.gz" >&2
  exit 1
fi
log "Using cmake: $CMAKE_CMD ($($CMAKE_CMD --version | head -1))"

# Download source tarball if missing
if [[ ! -s "$TARBALL" ]]; then
  log "Downloading librdkafka $RDKAFKA_VERSION..."
  if command -v wget >/dev/null 2>&1; then
    wget -q --show-progress -O "$TARBALL" "$TARBALL_URL" || { rm -f "$TARBALL"; exit 1; }
  elif command -v curl >/dev/null 2>&1; then
    curl -sSL -o "$TARBALL" "$TARBALL_URL" || { rm -f "$TARBALL"; exit 1; }
  else
    echo "ERROR: neither wget nor curl available" >&2
    exit 1
  fi
fi

# Clean and extract
rm -rf "$SRC_DIR" "$BUILD_DIR"
log "Extracting $TARBALL..."
tar -C "$BUILD_PARENT" -xzf "$TARBALL"

# Configure — minimal build: no SSL, SASL, zstd, plugins, tests, or examples.
# snappy is bundled inside librdkafka and does not need an external flag.
log "Configuring..."
"$CMAKE_CMD" -S "$SRC_DIR" -B "$BUILD_DIR" \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_INSTALL_PREFIX="$INSTALL_DIR" \
  -DCMAKE_INSTALL_LIBDIR=lib \
  -DWITH_SSL=OFF \
  -DWITH_SASL=OFF \
  -DWITH_ZSTD=OFF \
  -DWITH_PLUGINS=OFF \
  -DRDKAFKA_BUILD_TESTS=OFF \
  -DRDKAFKA_BUILD_EXAMPLES=OFF \
  2>&1 | tail -8

log "Building ($(nproc) cores)..."
"$CMAKE_CMD" --build "$BUILD_DIR" -j"$(nproc)" 2>&1 | tail -5

log "Installing to $INSTALL_DIR..."
"$CMAKE_CMD" --install "$BUILD_DIR" 2>&1 | tail -5

# Verify
[[ -f "$INSTALL_DIR/lib/librdkafka.so" ]] || { echo "ERROR: librdkafka.so not found after install" >&2; exit 1; }
[[ -f "$INSTALL_DIR/include/librdkafka/rdkafka.h" ]] || { echo "ERROR: rdkafka.h not found after install" >&2; exit 1; }

# Stamp
echo "$RDKAFKA_VERSION" > "$STAMP_FILE"
log "SUCCESS: librdkafka $RDKAFKA_VERSION installed to $INSTALL_DIR"
