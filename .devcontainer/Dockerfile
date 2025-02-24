FROM  jhginn/devcontainer:ubuntu2204-pyuv311-20250207-just

ENV RUFF_CACHE_DIR=$HOME/workspace/.cache/ruff
ENV MYPY_CACHE_DIR=$HOME/workspace/.cache/mypy
ENV PRE_COMMIT_HOME=$HOME/workspace/.cache/pre-commit

RUN pipx install --force uv==0.6.2
