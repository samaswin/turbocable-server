# Agent Environment Notes

## Development Environment

This project runs on **Windows 11** with development tooling inside **WSL (Ubuntu 24.04 LTS)**.
Rust is managed via **asdf** inside WSL — there is no native Windows Rust installation.

## Running Cargo / Rust Commands

All `cargo`, `rustc`, and related commands **must be run through WSL**. The correct invocation from the Windows host (e.g. from Claude Code's Bash tool) is:

```bash
wsl bash -ic "cd /mnt/c/Users/aswin/Git/turbocable-server && <command>"
```

- `-i` — interactive shell, so `.bashrc` is sourced and asdf shims are on `$PATH`
- `-c` — run the given command string

**Examples:**

```bash
# Build
wsl bash -ic "cd /mnt/c/Users/aswin/Git/turbocable-server && cargo build 2>&1"

# Run tests
wsl bash -ic "cd /mnt/c/Users/aswin/Git/turbocable-server && cargo test 2>&1"

# Clippy
wsl bash -ic "cd /mnt/c/Users/aswin/Git/turbocable-server && cargo clippy -- -D warnings 2>&1"

# Format check
wsl bash -ic "cd /mnt/c/Users/aswin/Git/turbocable-server && cargo fmt --check 2>&1"
```

## Path Mapping

| Context   | Path                                        |
|-----------|---------------------------------------------|
| Windows   | `C:\Users\aswin\Git\turbocable-server`      |
| WSL       | `/mnt/c/Users/aswin/Git/turbocable-server`  |

## Tool Versions (from `.tool-versions`)

| Tool  | Version  | Managed by |
|-------|----------|------------|
| Rust  | stable   | asdf (WSL) |

- asdf: v0.14.0
- Rust stable: 1.94.0 (as of 2026-03-19)
- cargo: at `/home/aswin/.asdf/shims/cargo` inside WSL
