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

Use judgment: **do not** run every check on every tiny task (that wastes time, especially `cargo doc` and `cargo audit`). Run what matches the change.

### After Rust code changes (default)

1. **Format check** — fix if needed:

```bash
wsl bash -ic "cd /mnt/c/Users/aswin/Git/turbocable-server && cargo fmt --all --check 2>&1"
```

If the check fails:

```bash
wsl bash -ic "cd /mnt/c/Users/aswin/Git/turbocable-server && cargo fmt --all 2>&1"
```

2. **Clippy** — fix reported issues:

```bash
wsl bash -ic "cd /mnt/c/Users/aswin/Git/turbocable-server && cargo clippy --all-targets --all-features -- -D warnings 2>&1"
```

3. **Tests** when behavior changed or could regress:

```bash
wsl bash -ic "cd /mnt/c/Users/aswin/Git/turbocable-server && cargo test 2>&1"
```

Skip clippy/tests only for edits that cannot affect them (e.g. typo in a comment with no code change).

### `cargo doc` — when relevant, not every time

Run **only if** the task adds or changes **rustdoc**, public API surface, or features that affect generated docs:

```bash
wsl bash -ic "cd /mnt/c/Users/aswin/Git/turbocable-server && cargo doc 2>&1"
```

For a quick check on **this crate only** (faster):

```bash
wsl bash -ic "cd /mnt/c/Users/aswin/Git/turbocable-server && cargo doc --no-deps 2>&1"
```

### `cargo audit` — when dependencies change

Run **only if** `Cargo.toml` / `Cargo.lock` / dependency versions were changed, or the task explicitly concerns security/supply chain:

```bash
wsl bash -ic "cd /mnt/c/Users/aswin/Git/turbocable-server && cargo audit 2>&1"
```

If vulnerabilities are found, fix by updating affected dependencies (`cargo update`, or edit `Cargo.toml` for major bumps), then re-run `cargo audit` to confirm resolution.

## Documentation and README Updates

When a task **changes behavior, config, or public API**, update the docs that cover it. Do not churn README or other `.md` files for unrelated edits.

1. Review `README.md` — update setup steps, feature lists, configuration examples, or usage instructions if affected.
2. Review any `.md` files in the project root (e.g. `gateway_phases.md`, `AGENTS.md`) — update if the task changes architecture, phases, or agent behavior.
3. If a new feature or component was added, ensure it is reflected in the relevant docs.

When nothing user-facing changed, skip doc edits.

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
