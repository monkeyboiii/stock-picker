# Linting Guide

This project uses **Ruff** for linting with automatic pre-commit hooks.

## Philosophy

Our linting is **minimal and non-invasive**:
- ✅ **DO**: Organize imports, remove trailing whitespace, catch syntax errors
- ❌ **DON'T**: Enforce line length, change indentation, reformat code style

Your editor's formatting is respected. Linters only catch real issues.

---

## Quick Start

### 1. Install Pre-commit Hooks

After cloning the repository:

```bash
uv sync  # Install dependencies including pre-commit
uv run pre-commit install  # Install git hooks
```

**That's it!** Linting now runs automatically on every `git commit`.

### 2. Make a Commit

```bash
git add .
git commit -m "Your commit message"
```

The pre-commit hook will:
1. ✅ Organize imports (via Ruff)
2. ✅ Remove trailing whitespace
3. ✅ Check for syntax errors
4. ✅ Ensure files end with newline

If fixes are made, the commit is **rejected** and you need to:
```bash
git add .  # Stage the auto-fixes
git commit -m "Your commit message"  # Commit again
```

### 3. Skip Hooks (When Needed)

To bypass pre-commit hooks:
```bash
git commit --no-verify -m "WIP: quick commit"
```

---

## Manual Linting

### Run Linter on All Files

```bash
# Check for issues
uv run ruff check .

# Check and auto-fix
uv run ruff check . --fix
```

### Run Linter on Specific Files

```bash
uv run ruff check app/main.py
uv run ruff check app/ --fix
```

### Run Pre-commit Manually

```bash
# Run all pre-commit hooks
uv run pre-commit run --all-files

# Run specific hook
uv run pre-commit run ruff --all-files
uv run pre-commit run trailing-whitespace --all-files
```

---

## What Gets Checked?

### ✅ Enabled Checks

| Check | Description | Example |
|-------|-------------|---------|
| **F** (Pyflakes) | Syntax errors, undefined names | `NameError`, unused imports |
| **I** (isort) | Import organization | Alphabetical, grouped correctly |
| **W291-293** | Trailing whitespace | Removes spaces at end of lines |
| **UP** (pyupgrade) | Modern Python syntax | `list[str]` instead of `List[str]` |

### ❌ Disabled Checks

| Check | Why Disabled |
|-------|--------------|
| Line length (E501) | Let developers choose their line length |
| Indentation | Respect your editor's indentation |
| Code style (E, W) | Too opinionated - trust developers |
| Complexity (C901) | Context-dependent |

---

## Configuration

All configuration is in `pyproject.toml`:

```toml
[tool.ruff.lint]
select = [
    "F",   # pyflakes (syntax errors, undefined names)
    "I",   # isort (import organization)
    "W291", "W292", "W293",  # trailing whitespace
    "UP",  # pyupgrade (modern Python syntax)
]
```

### Pre-commit Configuration

Pre-commit hooks are configured in `.pre-commit-config.yaml`:
- **Ruff**: Auto-fixes imports and whitespace
- **Trailing whitespace**: Removes trailing spaces
- **End-of-file fixer**: Ensures newline at EOF
- **Merge conflict checker**: Prevents accidental conflict markers
- **JSON/YAML checker**: Validates syntax

---

## Common Workflows

### First Time Setup

```bash
# Clone repo
git clone <repo-url>
cd stock-picker

# Install dependencies
uv sync

# Install git hooks
uv run pre-commit install
```

### Daily Development

```bash
# Make changes
vim app/main.py

# Commit (linting runs automatically)
git commit -am "Add new feature"

# If linter fixes issues, re-commit
git add .
git commit -m "Add new feature"
```

### Before Pushing

```bash
# Run all checks manually
uv run pre-commit run --all-files

# Run tests
uv run pytest
```

### Update Pre-commit Hooks

```bash
# Update to latest hook versions
uv run pre-commit autoupdate

# Re-run on all files
uv run pre-commit run --all-files
```

---

## Troubleshooting

### "pre-commit: command not found"

Install pre-commit:
```bash
uv sync  # Includes pre-commit in dev dependencies
```

### Hooks Not Running

Reinstall hooks:
```bash
uv run pre-commit install
```

### Hook Fails on Large Files

Pre-commit checks for files >500KB. If intentional:
```bash
git commit --no-verify
```

Or update `.pre-commit-config.yaml`:
```yaml
- id: check-added-large-files
  args: [--maxkb=1000]  # Increase limit to 1MB
```

### Import Organization Issues

If Ruff's import organization conflicts with your style:
```bash
# Disable for specific file
git add file.py
git commit --no-verify
```

Or adjust `.pre-commit-config.yaml` to skip that file.

---

## CI/CD Integration

### GitHub Actions

Pre-merge tests run automatically on pull requests to `main`:

```yaml
# .github/workflows/test.yml
- name: Run unit tests
  run: uv run pytest -v --tb=short -m "not integration"
```

**Tests run on**:
- Pull requests to `main`
- Direct pushes to `main`

**GitHub Free Tier**:
- ✅ 2,000 minutes/month free
- ✅ This workflow uses ~2-3 minutes per run
- ✅ Easily within free tier for normal development

### Status Checks

Configure branch protection rules on GitHub:
1. Go to **Settings** → **Branches**
2. Add rule for `main` branch
3. Enable: **"Require status checks to pass before merging"**
4. Select: **"Tests"** workflow
5. Enable: **"Allow force pushes"** (if needed)

---

## Best Practices

### DO

✅ Let linters auto-fix imports and whitespace
✅ Run `pre-commit run --all-files` before pushing
✅ Use `--no-verify` sparingly for WIP commits
✅ Keep your editor's indentation settings

### DON'T

❌ Fight with the linter - it only checks real issues
❌ Disable hooks permanently - they're lightweight
❌ Reformat entire files manually - trust your editor
❌ Commit without testing first

---

## Editor Integration

### VS Code

Install **Ruff** extension:
```json
// settings.json
{
  "ruff.lint.run": "onSave",
  "ruff.organizeImports": true
}
```

### PyCharm

Install **Ruff** plugin from marketplace.

### Vim/Neovim

Use **ALE** or **nvim-lint**:
```lua
require('lint').linters_by_ft = {
  python = {'ruff'},
}
```

---

**Last Updated:** 2025-11-17
**Ruff Version:** 0.9.10
**Pre-commit Version:** 4.0.0+
