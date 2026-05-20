#!/usr/bin/env bash
set -euo pipefail

REPO="sjalq/swarm"
BIN_NAME="swarm"
BIN_DIR="${BIN_DIR:-$HOME/.local/bin}"

info() { printf '  \033[1;34m>\033[0m %s\n' "$*"; }
err()  { printf '  \033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }

detect_os() {
    case "$(uname -s)" in
        Linux*)  echo "linux" ;;
        Darwin*) echo "darwin" ;;
        *)       err "Unsupported OS: $(uname -s)" ;;
    esac
}

detect_arch() {
    case "$(uname -m)" in
        x86_64|amd64)       echo "x86_64" ;;
        aarch64|arm64)      echo "aarch64" ;;
        *)                  err "Unsupported architecture: $(uname -m)" ;;
    esac
}

resolve_target() {
    local os="$1"
    local arch="$2"
    case "${os}-${arch}" in
        linux-x86_64)   echo "x86_64-unknown-linux-gnu" ;;
        linux-aarch64)  echo "aarch64-unknown-linux-gnu" ;;
        darwin-x86_64)  echo "x86_64-apple-darwin" ;;
        darwin-aarch64) echo "aarch64-apple-darwin" ;;
        *)              err "No prebuilt binary for ${os}/${arch}" ;;
    esac
}

resolve_version() {
    if [ -n "${SWARM_VERSION:-}" ]; then
        echo "$SWARM_VERSION"
        return
    fi

    info "Fetching latest release version..."
    local api_url="https://api.github.com/repos/${REPO}/releases/latest"
    local response
    response="$(curl -fsSL "$api_url" 2>/dev/null)" || err "Failed to fetch latest release from GitHub API"

    local version
    version="$(printf '%s' "$response" | grep '"tag_name"' | sed 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/')"

    if [ -z "$version" ]; then
        err "Could not determine latest version from GitHub API"
    fi
    echo "$version"
}

checksum_cmd() {
    local os="$1"
    case "$os" in
        darwin) echo "shasum -a 256" ;;
        linux)  echo "sha256sum" ;;
    esac
}

main() {
    local os arch target version
    os="$(detect_os)"
    arch="$(detect_arch)"
    target="$(resolve_target "$os" "$arch")"
    version="$(resolve_version)"

    local version_num="${version#v}"
    local archive="${BIN_NAME}-${version_num}-${target}.tar.gz"
    local base_url="https://github.com/${REPO}/releases/download/${version}"
    local archive_url="${base_url}/${archive}"
    local checksums_url="${base_url}/SHA256SUMS"

    info "Installing ${BIN_NAME} ${version} for ${target}"

    local tmp_dir
    tmp_dir="$(mktemp -d)"
    trap 'rm -rf "$tmp_dir"' EXIT

    info "Downloading ${archive}..."
    curl -fsSL -o "${tmp_dir}/${archive}" "$archive_url" || err "Download failed: ${archive_url}"

    info "Downloading checksums..."
    curl -fsSL -o "${tmp_dir}/SHA256SUMS" "$checksums_url" || err "Download failed: ${checksums_url}"

    info "Verifying checksum..."
    local expected_checksum
    expected_checksum="$(grep "${archive}" "${tmp_dir}/SHA256SUMS" | awk '{print $1}')"
    if [ -z "$expected_checksum" ]; then
        err "Archive ${archive} not found in SHA256SUMS"
    fi

    local sha_cmd
    sha_cmd="$(checksum_cmd "$os")"
    local actual_checksum
    actual_checksum="$(cd "$tmp_dir" && $sha_cmd "$archive" | awk '{print $1}')"

    if [ "$expected_checksum" != "$actual_checksum" ]; then
        err "Checksum mismatch!\n  expected: ${expected_checksum}\n  actual:   ${actual_checksum}"
    fi
    info "Checksum OK"

    info "Extracting to ${BIN_DIR}..."
    mkdir -p "$BIN_DIR"
    tar -xzf "${tmp_dir}/${archive}" -C "$tmp_dir"
    install -m 755 "${tmp_dir}/${BIN_NAME}" "${BIN_DIR}/${BIN_NAME}"

    info "Installed ${BIN_NAME} to ${BIN_DIR}/${BIN_NAME}"

    if ! echo "$PATH" | tr ':' '\n' | grep -qx "$BIN_DIR"; then
        printf '\n'
        info "WARNING: %s is not in your PATH." "$BIN_DIR"
        info "Add it with:"
        info "  export PATH=\"%s:\$PATH\"" "$BIN_DIR"
        printf '\n'
    fi

    info "Done! Run '${BIN_NAME} --help' to get started."
}

main
