#!/usr/bin/env bash
# Install simlock as a user-level Claude Code skill via a symlink.
set -euo pipefail

force=0
for arg in "$@"; do
    case "$arg" in
        -f|--force) force=1 ;;
        -h|--help)
            echo "usage: $0 [--force]"
            echo "Symlinks this repository to \${CLAUDE_SKILLS_DIR:-~/.claude/skills}/simlock"
            exit 0
            ;;
        *)
            echo "unknown argument: $arg" >&2
            exit 64
            ;;
    esac
done

source_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
skills_dir="${CLAUDE_SKILLS_DIR:-$HOME/.claude/skills}"
target="$skills_dir/simlock"

if [ ! -f "$source_dir/scripts/simlock.py" ]; then
    echo "error: $source_dir/scripts/simlock.py is missing" >&2
    exit 1
fi

mkdir -p "$skills_dir"

if [ -L "$target" ]; then
    current="$(cd "$target" && pwd -P)"
    if [ "$current" = "$source_dir" ]; then
        echo "simlock is already installed: $target -> $source_dir"
        exit 0
    fi
    if [ "$force" -ne 1 ]; then
        echo "error: $target is a symlink to $current" >&2
        echo "re-run with --force to replace it" >&2
        exit 1
    fi
    rm "$target"
elif [ -e "$target" ]; then
    if [ "$force" -ne 1 ]; then
        echo "error: $target already exists and is not a symlink" >&2
        echo "move it aside, or re-run with --force to replace it" >&2
        exit 1
    fi
    rm -rf "$target"
fi

ln -s "$source_dir" "$target"
echo "installed: $target -> $source_dir"

if command -v python3 >/dev/null 2>&1; then
    version="$(python3 "$target/scripts/simlock.py" --version)"
    echo "verified: $version"
else
    echo "warning: python3 not found on PATH" >&2
fi
