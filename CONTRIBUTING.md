# Contributing to gtfs-zone-rt-archive

## Commit Message Guidelines

This project uses [Conventional Commits](https://www.conventionalcommits.org/), enforced by the commit-msg hook. Use `cz commit` ([Commitizen](https://commitizen-tools.github.io/commitizen/), `uv tool install commitizen`) or plain `git commit` with a valid message:

```
<type>(<scope>): <subject>
```

Types: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`, `revert`. Add `!` after the type or `BREAKING CHANGE:` in the body for a major bump.

## Development Workflow

1. Create a feature branch from `main`
2. Make your changes
3. Commit with a conventional message
4. Push your branch and create a pull request

## Git Hooks

Hooks are defined in `.pre-commit-config.yaml` and run by [prek](https://github.com/j178/prek). Install them once per clone:

```bash
uv tool install prek
prek install
```

- **commit-msg**: Validates commit message format using commitizen
