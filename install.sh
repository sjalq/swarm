#!/usr/bin/env bash
set -euo pipefail

REPO="sjalq/swarm"
BIN_NAME="swarm"
BIN_DIR="${BIN_DIR:-$HOME/.local/bin}"
INSTALL_MODE="${SWARM_INSTALL:-auto}"
SOURCE_DIR="${SWARM_SOURCE_DIR:-}"

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

usage() {
    cat <<'EOF'
Install swarm.

Usage:
  install.sh [--release | --source | --local [PATH]] [--version vX.Y.Z] [--bin-dir PATH]

Modes:
  --release   Install a GitHub release only.
  --source    Build the latest GitHub source with cargo.
  --local     Build this checkout, or PATH when provided.

Default mode uses a release when available and falls back to source.
EOF
}

parse_args() {
    while [ "$#" -gt 0 ]; do
        case "$1" in
            --release)
                INSTALL_MODE="release"
                ;;
            --source)
                INSTALL_MODE="source"
                ;;
            --local)
                INSTALL_MODE="local"
                if [ "${2:-}" != "" ] && [ "${2#-}" = "$2" ]; then
                    SOURCE_DIR="$2"
                    shift
                fi
                ;;
            --version)
                [ "${2:-}" != "" ] || err "--version requires a value"
                SWARM_VERSION="$2"
                shift
                ;;
            --bin-dir)
                [ "${2:-}" != "" ] || err "--bin-dir requires a value"
                BIN_DIR="$2"
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                err "Unknown option: %s" "$1"
                ;;
        esac
        shift
    done

    case "$INSTALL_MODE" in
        auto|release|source|local) ;;
        *) err "SWARM_INSTALL must be auto, release, source, or local" ;;
    esac
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
    local libc="${3:-gnu}"
    case "${os}-${arch}-${libc}" in
        linux-x86_64-gnu)    echo "x86_64-unknown-linux-gnu" ;;
        linux-aarch64-gnu)   echo "aarch64-unknown-linux-gnu" ;;
        linux-x86_64-musl)   echo "x86_64-unknown-linux-musl" ;;
        darwin-x86_64-*)     echo "x86_64-apple-darwin" ;;
        darwin-aarch64-*)    echo "aarch64-apple-darwin" ;;
        *)                   return 1 ;;
    esac
}

is_musl_linux() {
    command -v ldd >/dev/null 2>&1 && ldd /bin/ls 2>&1 | grep -qi musl
}

detect_libc() {
    local os="$1"
    if [ "$os" = "linux" ] && is_musl_linux; then
        echo "musl"
    else
        echo "gnu"
    fi
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

ensure_on_path() {
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
            rc_file=""
            path_line="export PATH=\"${bin_dir}:\$PATH\""
            ;;
    esac

    if [ -z "$rc_file" ]; then
        printf '\n'
        info "WARNING: %s is not in your PATH." "$bin_dir"
        info "Add this to your shell startup file:"
        printf '\n  %s\n\n' "$path_line"
        return
    fi

    touch "$rc_file"
    if grep -qF "$bin_dir" "$rc_file" 2>/dev/null; then
        info "%s already referenced in %s" "$bin_dir" "$rc_file"
    else
        printf '\n# Added by swarm installer\n%s\n' "$path_line" >> "$rc_file"
        info "Added %s to %s" "$bin_dir" "$rc_file"
    fi

    export PATH="${bin_dir}:$PATH"
    info "%s is now on your PATH (current session and future shells)." "$bin_dir"
}

ensure_source_tools() {
    if ! command -v cargo >/dev/null 2>&1; then
        err "Cargo is required for source installs. Install Rust/Cargo from https://rustup.rs and retry."
    fi

    info "Ensuring wasm32 target is installed..."
    if command -v rustup >/dev/null 2>&1; then
        rustup target add wasm32-unknown-unknown 2>/dev/null || true
    else
        info "rustup not found; assuming wasm32-unknown-unknown is already installed."
    fi

    if ! command -v trunk >/dev/null 2>&1; then
        info "Installing trunk (needed to build the dashboard)..."
        cargo install trunk --locked || err "Failed to install trunk. Install manually with: cargo install trunk --locked"
    fi
}

verify_and_finish() {
    local os="$1"
    local bin_path="$2"

    info "Verifying installed binary..."
    if "$bin_path" --help >/dev/null 2>&1; then
        info "Verified: swarm is installed at %s" "$bin_path"
    else
        err "Installed binary did not run successfully:\n  %s --help" "$bin_path"
    fi

    local bin_dir
    bin_dir="$(dirname "$bin_path")"
    if ! echo "$PATH" | tr ':' '\n' | grep -qx "$bin_dir"; then
        ensure_on_path "$os" "$bin_dir"
    fi

    info "Done! Run '${BIN_NAME} --help' to get started."
}

install_from_git_source() {
    local os="$1"
    local reason="$2"

    info "%s" "$reason"
    ensure_source_tools

    local tmp_root
    tmp_root="$(mktemp -d)"

    info "Building latest source from GitHub (this takes a few minutes)..."
    cargo install --git "https://github.com/${REPO}" --locked --root "$tmp_root" swarm-cli \
        || err "Cargo source install failed.\nRetry manually with:\n  cargo install --git https://github.com/${REPO} --locked swarm-cli"

    mkdir -p "$BIN_DIR"
    install -m 755 "${tmp_root}/bin/${BIN_NAME}" "${BIN_DIR}/${BIN_NAME}"
    rm -rf "$tmp_root"
    verify_and_finish "$os" "${BIN_DIR}/${BIN_NAME}"
    exit 0
}

install_from_local_source() {
    local os="$1"
    local source_dir="$SOURCE_DIR"

    if [ -z "$source_dir" ]; then
        local script_path="${BASH_SOURCE[0]:-$0}"
        source_dir="$(cd "$(dirname "$script_path")" && pwd)"
    fi

    [ -f "${source_dir}/Cargo.toml" ] || err "Local source directory does not contain Cargo.toml: %s" "$source_dir"
    ensure_source_tools

    info "Building local checkout at %s" "$source_dir"
    (cd "$source_dir" && cargo build --release --locked) || err "Local cargo build failed."

    mkdir -p "$BIN_DIR"
    install -m 755 "${source_dir}/target/release/${BIN_NAME}" "${BIN_DIR}/${BIN_NAME}"
    verify_and_finish "$os" "${BIN_DIR}/${BIN_NAME}"
    exit 0
}

fallback_to_source_install() {
    local os="$1"
    local reason="$2"

    if [ "$INSTALL_MODE" = "release" ]; then
        err "%s" "$reason"
    fi

    install_from_git_source "$os" "$reason"
}

main() {
    parse_args "$@"

    local os arch libc target version
    os="$(detect_os)"
    arch="$(detect_arch)"
    libc="$(detect_libc "$os")"

    if [ "$INSTALL_MODE" = "local" ]; then
        install_from_local_source "$os"
    fi

    if [ "$INSTALL_MODE" = "source" ]; then
        install_from_git_source "$os" "Building from GitHub source because source mode was requested."
    fi

    if ! target="$(resolve_target "$os" "$arch" "$libc")"; then
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

    verify_and_finish "$os" "${BIN_DIR}/${BIN_NAME}"
}

main "$@"
