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

## Task Completion Requirement

After completing any task, **all agents must run the format check** and fix any issues before considering the task done:

```bash
wsl bash -ic "cd /mnt/c/Users/aswin/Git/turbocable-server && cargo fmt --all --check 2>&1"
```

If the check fails, run `cargo fmt --all` to fix formatting, then re-verify:

```bash
wsl bash -ic "cd /mnt/c/Users/aswin/Git/turbocable-server && cargo fmt --all 2>&1"
```

Also run `cargo clippy` to catch lint errors and warnings:

```bash
wsl bash -ic "cd /mnt/c/Users/aswin/Git/turbocable-server && cargo clippy --all-targets --all-features -- -D warnings 2>&1"
```

If clippy reports errors, fix them before considering the task done.

Also run `cargo doc` to ensure documentation builds without errors:

```bash
wsl bash -ic "cd /mnt/c/Users/aswin/Git/turbocable-server && cargo doc 2>&1"
```

Also run `cargo audit` to check for known vulnerabilities in dependencies:

```bash
wsl bash -ic "cd /mnt/c/Users/aswin/Git/turbocable-server && cargo audit 2>&1"
```

If vulnerabilities are found, fix them by updating the affected dependencies:

```bash
wsl bash -ic "cd /mnt/c/Users/aswin/Git/turbocable-server && cargo update 2>&1"
```

If `cargo update` does not resolve the vulnerability (e.g. it requires a major version bump), manually update the dependency version in `Cargo.toml` and run `cargo update` again. Re-run `cargo audit` to confirm all vulnerabilities are resolved before considering the task done.

## Documentation and README Updates

After completing any task, **always check and update documentation** if the task introduced new features, changed behavior, or affected any documented area:

1. Review `README.md` — update setup steps, feature lists, configuration examples, or usage instructions if affected.
2. Review any `.md` files in the project root (e.g. `gateway_phases.md`, `AGENTS.md`) — update if the task changes architecture, phases, or agent behavior.
3. If a new feature or component was added, ensure it is reflected in the relevant docs.

Do **not** skip this step. Documentation should always stay in sync with the code.

## Installation Requirements

If a task requires installing new tools, packages, or dependencies (e.g. `cargo install`, `apt install`, system tools), the agent **must not attempt the installation itself**. Instead:

1. Identify what needs to be installed and why.
2. Provide the exact commands and steps the user should run.
3. Ask the user to perform the installation, then resume the task once confirmed.

**Example:**
> "This task requires `cargo-expand`. Please run the following in WSL, then let me know when done:"
> ```bash
> wsl bash -ic "cargo install cargo-expand"
> ```

## Tool Versions (from `.tool-versions`)

| Tool  | Version  | Managed by |
|-------|----------|------------|
| Rust  | stable   | asdf (WSL) |

- asdf: v0.14.0
- Rust stable: 1.94.0 (as of 2026-03-19)
- cargo: at `/home/aswin/.asdf/shims/cargo` inside WSL
