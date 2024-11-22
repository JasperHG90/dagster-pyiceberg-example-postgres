alias s := setup
alias t := test
alias p := pre_commit
alias dd := dagster_dev
alias cn := create_air_quality_namespace
alias clz := create_landing_zone_bucket

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

# Create the 'air_quality' namespace in the pyiceberg catalog
create_air_quality_namespace:
  uv run python scripts/create_catalog_namespace.py

# Create landing zone bucket in Minio S3
create_landing_zone_bucket:
  uv run python scripts/create_landing_zone_bucket.py

# Run dagster dev
dagster_dev:
  #!/usr/bin/env bash
  set -eo pipefail
  mkdir -p .dagster
  cp dagster.yaml .dagster/dagster.yaml
  export DAGSTER_HOME=$(pwd)/.dagster
  uv run dagster dev -d /home/vscode/workspace -m luchtmeetnet
