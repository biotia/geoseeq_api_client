# Project AGENTS.md Guide for OpenAI Codex

This AGENTS.md file provides guidance for OpenAI Codex and other AI agents working with this Python codebase.

## Project Structure for OpenAI Codex Navigation

- `/geoseeq`: Python package with CLI and API client implementation
- `/docs`: Documentation and usage examples
- `/tests`: Pytest suite that should be maintained and extended

## Coding Conventions for OpenAI Codex

### General Conventions

- Use Python 3.8+ for all new code
- Format code with **black** (line length 100) and **isort**
- Lint using **pylint** with `commit_pylintrc`
- Keep functions small and use descriptive names
- Add docstrings for complex logic

### CLI and Module Guidelines

- Place CLI related code under `geoseeq/cli`
- Keep modules focused on a single concern
- Provide type hints where practical

## Testing Requirements for OpenAI Codex

OpenAI Codex should run tests with:

```bash
pytest
pytest --cov=geoseeq
```

## Pull Request Guidelines for OpenAI Codex

1. Include a clear description of the changes
2. Reference any related issues
3. Ensure all tests and pre-commit hooks pass
4. Keep pull requests focused on a single concern

## Programmatic Checks for OpenAI Codex

Before submitting changes, run:

```bash
pre-commit run --files <modified-files>
pytest
```

All checks must pass before code can be merged.
