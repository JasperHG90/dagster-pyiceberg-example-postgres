alias s := setup
alias t := test
alias p := pre_commit
alias dd := dagster_dev

# Install python dependencies
install:
  uv sync

# Install pre-commit hooks
pre_commit_setup:
  uv run pre-commit install

# Install python dependencies and pre-commit hooks
setup: install pre_commit_setup

# Run pre-commit
pre_commit:
 uv run pre-commit run -a

# Run pytest
test:
  uv run pytest tests

# Run dagster dev
dagster_dev:
  #!/usr/bin/env bash
  set -eo pipefail
  mkdir -p .dagster
  cp dagster.yaml .dagster/dagster.yaml
  export DAGSTER_HOME=$(pwd)/.dagster
  uv run dagster dev -d /home/vscode/workspace -f /home/vscode/workspace/src/dagster_pyiceberg_example/__init__.py
