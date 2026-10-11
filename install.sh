#!/usr/bin/env bash
#
# FlareDB CLI installer (Linux and macOS)
#
#   curl -sSL https://install.flare-db.com | bash
#
# Options (environment variables):
#   VERSION=0.3.2        install a specific version instead of the latest
#   INSTALL_DIR=/path    install location (default: $HOME/.local/bin)
#   FLARE_BASE_URL=...   download from a different mirror
#   FORCE=1              reinstall even if this version is already installed
#
# The whole script is wrapped in main() and called on the last line, so a
# connection that drops mid-download can never run a half-downloaded script.

set -euo pipefail

BASE_URL="${FLARE_BASE_URL:-https://install.flare-db.com}"
REMOTE_DIR="cli"          # folder in the bucket
ARCHIVE_PREFIX="flare-cli" # archive file name prefix
BIN_NAME="flare"          # name of the executable inside the archive
INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"

say()  { printf '%s\n' "$*"; }
err()  { printf 'error: %s\n' "$*" >&2; exit 1; }
need() { command -v "$1" >/dev/null 2>&1 || err "'$1' is required but not installed"; }

download() { # url, output file
  if command -v curl >/dev/null 2>&1; then
    curl -fL --retry 3 --connect-timeout 15 --progress-bar "$1" -o "$2"
  else
    wget -q --show-progress -O "$2" "$1"
  fi
}

fetch_text() { # url -> stdout
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL --retry 3 --connect-timeout 15 "$1"
  else
    wget -qO- "$1"
  fi
}

detect_target() {
  local os arch
  os="$(uname -s)"
  arch="$(uname -m)"

  case "$os" in
    Darwin)
      # Under Rosetta, uname reports x86_64 even on Apple Silicon.
      if [ "$arch" = "x86_64" ] && [ "$(sysctl -n sysctl.proc_translated 2>/dev/null || echo 0)" = "1" ]; then
        arch="arm64"
      fi
      case "$arch" in
        arm64|aarch64) echo "aarch64-apple-darwin" ;;
        x86_64)        echo "x86_64-apple-darwin" ;;
        *) err "unsupported macOS architecture: $arch" ;;
      esac
      ;;
    Linux)
      if (ldd --version 2>&1 || true) | grep -qi musl; then
        err "musl-based Linux (e.g. Alpine) is not supported yet; the prebuilt binaries need glibc"
      fi
      case "$arch" in
        x86_64|amd64)  echo "x86_64-unknown-linux-gnu" ;;
        aarch64|arm64) echo "aarch64-unknown-linux-gnu" ;;
        *) err "unsupported Linux architecture: $arch" ;;
      esac
      ;;
    *) err "unsupported operating system: $os (only Linux and macOS are supported)" ;;
  esac
}

sha256_of() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  elif command -v shasum >/dev/null 2>&1; then
    shasum -a 256 "$1" | awk '{print $1}'
  else
    err "need 'sha256sum' or 'shasum' to verify the download"
  fi
}

print_path_hint() {
  case ":$PATH:" in
    *":$INSTALL_DIR:"*) return ;;
  esac
  say ""
  say "$INSTALL_DIR is not on your PATH. Add it with:"
  case "$(basename "${SHELL:-}")" in
    fish) say "  fish_add_path $INSTALL_DIR" ;;
    zsh)  say "  echo 'export PATH=\"$INSTALL_DIR:\$PATH\"' >> ~/.zshrc && source ~/.zshrc" ;;
    bash) say "  echo 'export PATH=\"$INSTALL_DIR:\$PATH\"' >> ~/.bashrc && source ~/.bashrc" ;;
    *)    say "  export PATH=\"$INSTALL_DIR:\$PATH\"" ;;
  esac
}

main() {
  need tar
  need xz
  need uname
  if ! command -v curl >/dev/null 2>&1 && ! command -v wget >/dev/null 2>&1; then
    err "'curl' or 'wget' is required"
  fi

  local target version archive tmp expected actual bin old_version
  target="$(detect_target)"

  if [ -n "${VERSION:-}" ]; then
    version="${VERSION#v}"
  else
    version="$(fetch_text "$BASE_URL/$REMOTE_DIR/latest.txt" | tr -d '[:space:]')" \
      || err "could not determine the latest version from $BASE_URL"
    [ -n "$version" ] || err "received an empty version from $BASE_URL/$REMOTE_DIR/latest.txt"
  fi

  # Version currently installed (empty if none), e.g. "0.3.2".
  old_version=""
  if [ -x "$INSTALL_DIR/$BIN_NAME" ]; then
    old_version="$("$INSTALL_DIR/$BIN_NAME" --version 2>/dev/null \
      | grep -oE '[0-9]+\.[0-9]+\.[0-9]+[0-9A-Za-z.+-]*' | head -n 1 || true)"
  fi

  if [ -n "$old_version" ] && [ "$old_version" = "$version" ] && [ -z "${FORCE:-}" ]; then
    if [ -n "${VERSION:-}" ]; then
      say "flare-cli $version is already installed."
    else
      say "You are on the latest version (flare-cli $version)."
    fi
    say "Location:  $INSTALL_DIR/$BIN_NAME"
    print_path_hint
    return 0
  fi

  archive="$ARCHIVE_PREFIX-$target.tar.xz"
  tmp="$(mktemp -d)"
  # Expand $tmp now (double quotes): main()'s locals are gone when EXIT fires.
  trap "rm -rf '$tmp'" EXIT

  say "Installing flare-cli $version"

  download "$BASE_URL/$REMOTE_DIR/$version/$archive" "$tmp/$archive" \
    || err "download failed: $BASE_URL/$REMOTE_DIR/$version/$archive"

  # Verify checksum (first field of the .sha256 file is the hash).
  expected="$(fetch_text "$BASE_URL/$REMOTE_DIR/$version/$archive.sha256" | awk '{print $1}' | head -n 1)" \
    || err "could not download the checksum file"
  actual="$(sha256_of "$tmp/$archive")"
  if [ "$expected" != "$actual" ]; then
    err "checksum mismatch (expected $expected, got $actual). Nothing was installed."
  fi

  mkdir -p "$tmp/extract"
  tar -xJf "$tmp/$archive" -C "$tmp/extract"

  # Find the binary wherever the archive put it (top-level or inside a folder).
  bin="$(find "$tmp/extract" -type f -name "$BIN_NAME" | head -n 1)"
  [ -n "$bin" ] || err "'$BIN_NAME' not found inside the archive"

  # Install atomically: copy next to the target, then rename into place.
  mkdir -p "$INSTALL_DIR"
  cp "$bin" "$INSTALL_DIR/.$BIN_NAME.new"
  chmod 755 "$INSTALL_DIR/.$BIN_NAME.new"
  mv -f "$INSTALL_DIR/.$BIN_NAME.new" "$INSTALL_DIR/$BIN_NAME"

  say ""
  if [ -z "$old_version" ]; then
    say "Installed: flare-cli $version"
  elif [ "$old_version" = "$version" ]; then
    say "Reinstalled: flare-cli $version"
  elif [ "$(printf '%s\n%s\n' "$old_version" "$version" | sort -V | head -n 1)" = "$version" ] \
       && [ "$old_version" != "$version" ]; then
    say "Downgraded: flare-cli $old_version -> flare-cli $version"
  else
    say "Upgraded: flare-cli $old_version -> flare-cli $version (new version)"
  fi
  say "Location:  $INSTALL_DIR/$BIN_NAME"

  print_path_hint
}

main "$@"
