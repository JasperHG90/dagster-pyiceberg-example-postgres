from pathlib import Path

from dagster_dbt import DbtProject

luchtmeetnet_models_project = DbtProject(
    project_dir=Path(__file__).joinpath("..", "dbt").resolve(),
    packaged_project_dir=Path(__file__).joinpath("..", "dbt-project").resolve(),
)
luchtmeetnet_models_project.prepare_if_dev()
