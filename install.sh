#!/bin/sh
# shellcheck shell=sh
#
# ez-cdc CLI installer
#
# Usage:
#   curl -sSL https://raw.githubusercontent.com/ez-cdc/dbmazz/main/install.sh | sh
#
# Environment variable overrides:
#   EZ_CDC_VERSION       Pin a specific release (e.g. v1.5.2). Default: latest.
#   EZ_CDC_INSTALL_DIR   Target install directory. Default: $HOME/.local/bin,
#                        falling back to /usr/local/bin (with sudo if needed).
#
# The installer downloads the correct ez-cdc binary for your OS and architecture
# from the GitHub releases page of ez-cdc/dbmazz, verifies the SHA256 checksum
# against the SHA256SUMS asset attached to the same release, and places the
# binary in your PATH.
#
# Supported platforms:
#   linux/amd64  linux/arm64  darwin/amd64  darwin/arm64

set -eu

GITHUB_REPO="ez-cdc/dbmazz"
BINARY_NAME="ez-cdc"

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

info() {
    printf '\033[1;34m==>\033[0m %s\n' "$*"
}

warn() {
    printf '\033[1;33mwarning:\033[0m %s\n' "$*" >&2
}

err() {
    printf '\033[1;31merror:\033[0m %s\n' "$*" >&2
    exit 1
}

have_cmd() {
    command -v "$1" >/dev/null 2>&1
}

require_cmd() {
    have_cmd "$1" || err "required command '$1' not found in PATH"
}

# ---------------------------------------------------------------------
# Platform detection
# ---------------------------------------------------------------------

detect_os() {
    uname_s=$(uname -s)
    case "$uname_s" in
        Linux)   echo "linux" ;;
        Darwin)  echo "darwin" ;;
        *)       err "unsupported OS: $uname_s (supported: Linux, Darwin)" ;;
    esac
}

detect_arch() {
    uname_m=$(uname -m)
    case "$uname_m" in
        x86_64|amd64)   echo "amd64" ;;
        aarch64|arm64)  echo "arm64" ;;
        *)              err "unsupported architecture: $uname_m (supported: x86_64, aarch64)" ;;
    esac
}

# ---------------------------------------------------------------------
# Version resolution
# ---------------------------------------------------------------------

resolve_version() {
    if [ -n "${EZ_CDC_VERSION:-}" ]; then
        echo "$EZ_CDC_VERSION"
        return
    fi
    api_url="https://api.github.com/repos/${GITHUB_REPO}/releases/latest"
    # Avoid needing jq: grep + cut on the JSON response.
    tag=$(curl -fsSL "$api_url" 2>/dev/null | grep -m1 '"tag_name"' | cut -d'"' -f4)
    [ -n "$tag" ] || err "could not resolve latest release from $api_url"
    echo "$tag"
}

# ---------------------------------------------------------------------
# Download + checksum verification
# ---------------------------------------------------------------------

download_binary() {
    version="$1"
    asset="$2"
    out_path="$3"

    asset_url="https://github.com/${GITHUB_REPO}/releases/download/${version}/${asset}"
    info "Downloading $asset_url"
    curl -fsSL -o "$out_path" "$asset_url" \
        || err "failed to download $asset_url"
}

verify_checksum() {
    version="$1"
    asset="$2"
    binary_path="$3"
    tmp_dir="$4"

    sums_url="https://github.com/${GITHUB_REPO}/releases/download/${version}/SHA256SUMS"
    sums_file="${tmp_dir}/SHA256SUMS"

    if ! curl -fsSL -o "$sums_file" "$sums_url" 2>/dev/null; then
        warn "SHA256SUMS not available at $sums_url — skipping checksum verification"
        return 0
    fi

    expected=$(grep " ${asset}\$" "$sums_file" | cut -d' ' -f1)
    if [ -z "$expected" ]; then
        warn "No checksum entry for $asset in SHA256SUMS — skipping verification"
        return 0
    fi

    if have_cmd sha256sum; then
        actual=$(sha256sum "$binary_path" | cut -d' ' -f1)
    elif have_cmd shasum; then
        actual=$(shasum -a 256 "$binary_path" | cut -d' ' -f1)
    else
        warn "neither sha256sum nor shasum available — skipping checksum verification"
        return 0
    fi

    if [ "$expected" != "$actual" ]; then
        err "checksum mismatch for $asset: expected $expected, got $actual"
    fi
    info "Checksum verified"
}

# ---------------------------------------------------------------------
# Install target selection
# ---------------------------------------------------------------------

choose_install_dir() {
    if [ -n "${EZ_CDC_INSTALL_DIR:-}" ]; then
        echo "$EZ_CDC_INSTALL_DIR"
        return
    fi

    # Prefer $HOME/.local/bin if it's in PATH or exists.
    local_bin="${HOME}/.local/bin"
    case ":${PATH}:" in
        *":${local_bin}:"*) echo "$local_bin"; return ;;
    esac
    if [ -d "$local_bin" ]; then
        echo "$local_bin"
        return
    fi

    # Fallback to /usr/local/bin (may need sudo).
    echo "/usr/local/bin"
}

install_binary() {
    src="$1"
    dest_dir="$2"
    dest="${dest_dir}/${BINARY_NAME}"

    mkdir -p "$dest_dir" 2>/dev/null || true

    if [ -w "$dest_dir" ]; then
        cp "$src" "$dest"
        chmod +x "$dest"
    else
        info "Installing to $dest_dir requires sudo"
        sudo cp "$src" "$dest"
        sudo chmod +x "$dest"
    fi

    echo "$dest"
}

check_path() {
    dest_dir="$1"
    case ":${PATH}:" in
        *":${dest_dir}:"*) return 0 ;;
    esac
    warn "$dest_dir is not in your PATH"
    printf '       Add this to your shell rc file:\n'
    # shellcheck disable=SC2016 # we want literal $PATH in the user-visible output
    printf '         export PATH="%s:$PATH"\n' "$dest_dir"
}

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

main() {
    require_cmd curl
    require_cmd uname
    require_cmd mkdir
    require_cmd cp

    os=$(detect_os)
    arch=$(detect_arch)
    version=$(resolve_version)
    asset="${BINARY_NAME}-${os}-${arch}"

    info "Platform:     ${os}/${arch}"
    info "Version:      ${version}"
    info "Asset:        ${asset}"

    tmp_dir=$(mktemp -d)
    # shellcheck disable=SC2064 # expand $tmp_dir now, not at trap time
    trap "rm -rf '$tmp_dir'" EXIT INT TERM

    binary_path="${tmp_dir}/${BINARY_NAME}"
    download_binary "$version" "$asset" "$binary_path"
    verify_checksum "$version" "$asset" "$binary_path" "$tmp_dir"
    chmod +x "$binary_path"

    dest_dir=$(choose_install_dir)
    info "Installing to ${dest_dir}"
    installed=$(install_binary "$binary_path" "$dest_dir")

    check_path "$dest_dir"

    info "Installed: $installed"
    if have_cmd "$BINARY_NAME"; then
        info "Verifying installation"
        "$BINARY_NAME" --version || warn "'$BINARY_NAME --version' did not succeed — binary may still work"
    fi

    # --- Next steps -----------------------------------------------------
    # Default config path follows XDG: $XDG_CONFIG_HOME/ez-cdc/config.yaml
    # (falls back to $HOME/.config/ez-cdc/config.yaml when the env var is
    # unset). Keep this message in sync with `resolve_config_path` in
    # `e2e-cli/src/main.rs`.
    xdg_base="${XDG_CONFIG_HOME:-${HOME}/.config}"
    config_path="${xdg_base}/ez-cdc/config.yaml"

    printf '\n'
    printf '\033[1;32m==>\033[0m Next steps\n'
    printf '\n'
    printf '    1. Create a starter config with every dbmazz option documented:\n'
    printf '       \033[36mez-cdc datasource init\033[0m\n'
    printf '       → writes %s\n' "$config_path"
    printf '\n'
    printf '    2. Either edit the file by hand, or add datasources interactively:\n'
    printf '       \033[36mez-cdc datasource add\033[0m\n'
    printf '\n'
    printf '    3. Validate a connection and run a pipeline end-to-end:\n'
    printf '       \033[36mez-cdc datasource test <name>\033[0m\n'
    printf '       \033[36mez-cdc quickstart --source <name> --sink <name>\033[0m\n'
    printf '\n'
    printf '    Docs: https://github.com/%s\n' "$GITHUB_REPO"
    printf '\n'
}

main "$@"
