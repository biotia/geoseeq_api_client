
## Local development

### Linting, formatting

Recommended linter is pylint, general formatter black and isort to format imports on save. To setup in your development enviroment run:

```sh
  pip install pylint black isort
```

Add the following lines to the settings.json:

```sh
  "python.linting.enabled": true,
  "python.linting.pylintEnabled": true,
  "python.linting.pylintArgs": [
    "--max-line-length=100",
  ],
  "python.sortImports.args": [
    "--profile", "black"
  ],
  "[python]": {
    "editor.codeActionsOnSave": {
        "source.organizeImports": true
    }
  },
  "python.formatting.provider": "black",
  "python.formatting.blackArgs": ["--line-length", "100"]
```

### Commit linting

We use conventional commits. [read](https://www.conventionalcommits.org)\
To setup pre-commit run:

```sh
pip install pre-commit
pre-commit install
pre-commit install --hook-type commit-msg
```