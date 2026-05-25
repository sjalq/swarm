#!/usr/bin/env bash
set -euo pipefail

REPO="sjalq/swarm"
BIN_NAME="swarm"
BIN_DIR="${BIN_DIR:-$HOME/.local/bin}"

info() {
    local fmt="$1"
    shift || true
    printf '  \033[1;34m>\033[0m '
    if [ "$#" -gt 0 ]; then
        printf "$fmt" "$@"
    else
        printf '%s' "$fmt"
    fi
    printf '\n'
}

err() {
    local fmt="$1"
    shift || true
    printf '  \033[1;31merror:\033[0m ' >&2
    if [ "$#" -gt 0 ]; then
        printf "$fmt" "$@" >&2
    else
        printf '%s' "$fmt" >&2
    fi
    printf '\n' >&2
    exit 1
}

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
        *)                  uname -m ;;
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
        *)              return 1 ;;
    esac
}

is_musl_linux() {
    command -v ldd >/dev/null 2>&1 && ldd /bin/ls 2>&1 | grep -qi musl
}

resolve_version() {
    if [ -n "${SWARM_VERSION:-}" ]; then
        echo "$SWARM_VERSION"
        return
    fi

    info "Fetching latest release version..."
    local api_url="https://api.github.com/repos/${REPO}/releases/latest"
    local response
    response="$(curl -fsSL "$api_url" 2>/dev/null)" || return 1

    local version
    version="$(printf '%s' "$response" | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1)"

    if [ -z "$version" ]; then
        return 1
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

print_path_hint() {
    local os="$1"
    local bin_dir="${2:-$BIN_DIR}"
    local shell_name="${SHELL##*/}"
    local rc_file
    local path_line

    case "$shell_name" in
        zsh)
            rc_file="$HOME/.zshrc"
            path_line="export PATH=\"${bin_dir}:\$PATH\""
            ;;
        bash)
            if [ "$os" = "darwin" ]; then
                rc_file="$HOME/.bash_profile"
            else
                rc_file="$HOME/.bashrc"
            fi
            path_line="export PATH=\"${bin_dir}:\$PATH\""
            ;;
        fish)
            rc_file="$HOME/.config/fish/config.fish"
            path_line="fish_add_path \"${bin_dir}\""
            ;;
        *)
            rc_file="your shell startup file"
            path_line="export PATH=\"${bin_dir}:\$PATH\""
            ;;
    esac

    printf '\n'
    info "WARNING: %s is not in your PATH." "$bin_dir"
    info "Add this line to %s:" "$rc_file"
    printf '\n  %s\n\n' "$path_line"
    info "Then restart your shell, or run the line above in this terminal."
    printf '\n'
}

fallback_to_source_install() {
    local os="$1"
    local reason="$2"

    info "%s" "$reason"
    if ! command -v cargo >/dev/null 2>&1; then
        err "No prebuilt release found and cargo is not installed.\nEither retry after a GitHub release is published, or install Rust/Cargo from https://rustup.rs and run:\n  cargo install --git https://github.com/sjalq/swarm swarm-cli"
    fi

    info "No prebuilt release found. Falling back to cargo source install (this takes a few minutes)..."
    cargo install --git https://github.com/sjalq/swarm swarm-cli || err "Cargo source install failed.\nRetry manually with:\n  cargo install --git https://github.com/sjalq/swarm swarm-cli"

    local cargo_bin_dir="${CARGO_HOME:-$HOME/.cargo}/bin"
    local installed_bin="${cargo_bin_dir}/${BIN_NAME}"
    local installed_version

    if installed_version="$("$installed_bin" --version 2>&1)"; then
        info "Verified: %s" "$installed_version"
    elif command -v "$BIN_NAME" >/dev/null 2>&1 && installed_version="$("$BIN_NAME" --version 2>&1)"; then
        info "Verified: %s" "$installed_version"
    else
        err "Source install finished, but the installed swarm binary could not be verified.\nCheck Cargo output and ensure %s is on your PATH." "$cargo_bin_dir"
    fi

    if ! echo "$PATH" | tr ':' '\n' | grep -qx "$cargo_bin_dir"; then
        print_path_hint "$os" "$cargo_bin_dir"
    fi

    info "Done! Run '${BIN_NAME} --help' to get started."
    exit 0
}

main() {
    local os arch target version
    os="$(detect_os)"
    arch="$(detect_arch)"

    if [ "$os" = "linux" ] && is_musl_linux; then
        fallback_to_source_install "$os" "musl Linux detected, but prebuilt musl binaries are not available yet."
    fi

    if ! target="$(resolve_target "$os" "$arch")"; then
        fallback_to_source_install "$os" "No prebuilt binary is available for ${os}/${arch}."
    fi

    if ! version="$(resolve_version)"; then
        fallback_to_source_install "$os" "Could not find a published GitHub release."
    fi

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
    curl -fsSL -o "${tmp_dir}/${archive}" "$archive_url" || fallback_to_source_install "$os" "Could not download prebuilt archive: ${archive_url}"

    info "Downloading checksums..."
    curl -fsSL -o "${tmp_dir}/SHA256SUMS" "$checksums_url" || fallback_to_source_install "$os" "Could not download release checksums: ${checksums_url}"

    info "Verifying checksum..."
    local expected_checksum
    expected_checksum="$(awk -v archive="$archive" '$2 == archive { print $1; exit }' "${tmp_dir}/SHA256SUMS")"
    if [ -z "$expected_checksum" ]; then
        fallback_to_source_install "$os" "Release checksums do not include ${archive}."
    fi

    local sha_cmd
    sha_cmd="$(checksum_cmd "$os")"
    local actual_checksum
    actual_checksum="$(cd "$tmp_dir" && $sha_cmd "$archive" | awk '{print $1}')"

    if [ "$expected_checksum" != "$actual_checksum" ]; then
        err "Checksum mismatch!\n  expected: %s\n  actual:   %s" "$expected_checksum" "$actual_checksum"
    fi
    info "Checksum OK"

    info "Extracting to ${BIN_DIR}..."
    mkdir -p "$BIN_DIR"
    tar -xzf "${tmp_dir}/${archive}" -C "$tmp_dir"
    install -m 755 "${tmp_dir}/${BIN_NAME}" "${BIN_DIR}/${BIN_NAME}"

    info "Installed ${BIN_NAME} to ${BIN_DIR}/${BIN_NAME}"

    info "Verifying installed binary..."
    local installed_version
    if installed_version="$("${BIN_DIR}/${BIN_NAME}" --version 2>&1)"; then
        info "Verified: %s" "$installed_version"
    else
        err "Installed binary did not run successfully:\n  %s --version\n%s" "${BIN_DIR}/${BIN_NAME}" "$installed_version"
    fi

    if ! echo "$PATH" | tr ':' '\n' | grep -qx "$BIN_DIR"; then
        print_path_hint "$os" "$BIN_DIR"
    fi

    info "Done! Run '${BIN_NAME} --help' to get started."
}

main
