#!/usr/bin/env bash
# Install verlet CLI into ~/.verlet/venv and symlink to ~/.local/bin/verlet
# Usage: curl -sSL https://raw.githubusercontent.com/verlet/verlet-cli/main/install.sh | bash
set -euo pipefail

INSTALL_DIR="$HOME/.verlet"
VENV_DIR="$INSTALL_DIR/venv"
BIN_DIR="$HOME/.local/bin"
PACKAGE="verlet"
REPO="https://github.com/verlet-robotics/verlet-cli.git"

info()  { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
ok()    { printf '\033[1;32m==>\033[0m %s\n' "$*"; }
err()   { printf '\033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }

# --- Check python ---
PYTHON=""
for cmd in python3 python; do
    if command -v "$cmd" &>/dev/null; then
        version=$("$cmd" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null || true)
        major=$(echo "$version" | cut -d. -f1)
        minor=$(echo "$version" | cut -d. -f2)
        if [ "$major" = "3" ] && [ "$minor" -ge 10 ]; then
            PYTHON="$cmd"
            break
        fi
    fi
done
[ -n "$PYTHON" ] || err "Python 3.10+ is required but not found. Install it first."
info "Using $PYTHON ($version)"

# --- Create venv ---
if [ -d "$VENV_DIR" ]; then
    info "Removing existing venv..."
    rm -rf "$VENV_DIR"
fi

info "Creating virtual environment at $VENV_DIR..."
"$PYTHON" -m venv "$VENV_DIR"

# --- Install ---
info "Installing verlet..."
"$VENV_DIR/bin/pip" install --quiet --upgrade pip
"$VENV_DIR/bin/pip" install --quiet "$PACKAGE" 2>/dev/null \
    || "$VENV_DIR/bin/pip" install --quiet "git+${REPO}"

# --- Symlink ---
mkdir -p "$BIN_DIR"
ln -sf "$VENV_DIR/bin/verlet" "$BIN_DIR/verlet"

# --- Check PATH ---
if ! echo "$PATH" | tr ':' '\n' | grep -qx "$BIN_DIR"; then
    SHELL_NAME=$(basename "$SHELL")
    case "$SHELL_NAME" in
        zsh)  RC="$HOME/.zshrc" ;;
        bash) RC="$HOME/.bashrc" ;;
        fish) RC="$HOME/.config/fish/config.fish" ;;
        *)    RC="" ;;
    esac

    if [ -n "$RC" ]; then
        if [ "$SHELL_NAME" = "fish" ]; then
            echo "fish_add_path $BIN_DIR" >> "$RC"
        else
            echo "export PATH=\"$BIN_DIR:\$PATH\"" >> "$RC"
        fi
        ok "Added $BIN_DIR to PATH in $RC"
        info "Restart your shell or run: export PATH=\"$BIN_DIR:\$PATH\""
    else
        info "Add $BIN_DIR to your PATH manually."
    fi
fi

ok "Installed! Run 'verlet login' to get started."
