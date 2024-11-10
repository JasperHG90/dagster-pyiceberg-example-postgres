"""
This is a custom dbt-duckdb plugin that allows the user to read and write to Iceberg tables from dbt.

This implementation builds on the default iceberg plugin from dbt-duckdb found here:
https://github.com/duckdb/dbt-duckdb/blob/master/dbt/adapters/duckdb/plugins/iceberg.py

It also depends on a number of helper functions from the dagster-pyiceberg library found here:
https://github.com/JasperHG90/dagster-pyiceberg

See:
- https://github.com/duckdb/dbt-duckdb/pull/141/files/fe8b9d191ef380f56f199057d8ec086372798dd4
- https://github.com/duckdb/dbt-duckdb/pull/141
- https://github.com/duckdb/dbt-duckdb?tab=readme-ov-file#using-local-python-modules
"""

from typing import Any, Dict, cast

import pyarrow.parquet as pq
import pyiceberg.catalog
from dbt.adapters.duckdb.plugins import BasePlugin
from dbt.adapters.duckdb.utils import SourceConfig, TargetConfig


class Plugin(BasePlugin):
    def initialize(self, config: Dict[str, Any]):
        if "catalog" not in config:
            raise Exception("'catalog' is a required argument for the iceberg plugin!")
        catalog = config.pop("catalog")
        namespace = (
            config.pop("namespace") if config.get("namespace") is not None else None
        )
        self._catalog = pyiceberg.catalog.load_catalog(catalog, **config)
        self._namespace = namespace

    def load(self, source_config: SourceConfig):
        table_format = source_config.get("iceberg_table", "{schema}.{identifier}")
        table_name = table_format.format(**source_config.as_dict())
        table = self._catalog.load_table(table_name)
        scan_keys = {
            "row_filter",
            "selected_fields",
            "case_sensitive",
            "snapshot_id",
            "options",
            "limit",
        }
        scan_config = {k: source_config[k] for k in scan_keys if k in source_config}
        return table.scan(**scan_config).to_arrow()

    def store(self, target_config: TargetConfig):
        # assert target_config.location is not None, "Location is required for storing data!"

        df = pq.read_table(target_config.location.path)

        config_ = cast(Dict[str, str], target_config.config)

        assert (
            target_config.relation.identifier is not None
        ), "Relation identifier is required to name the table!"

        table_name = target_config.relation.identifier
        namespace = (
            config_.get("namespace")
            if config_.get("namespace") is not None
            else self._namespace
        )

        assert namespace is not None, "Namespace is required to store data in Iceberg!"

        table_identifier = f"{namespace}.{table_name}"

        print(table_identifier)

        if self._catalog.table_exists(table_identifier):
            table = self._catalog.load_table(table_identifier)
        else:
            table = self._catalog.create_table_if_not_exists(
                table_identifier,
                schema=df.schema,
            )

        table.overwrite(
            df=df,
        )
