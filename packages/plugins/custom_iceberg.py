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

from typing import Any, Dict

import pyiceberg.catalog
from dbt.adapters.duckdb.plugins import BasePlugin
from dbt.adapters.duckdb.utils import SourceConfig


class Plugin(BasePlugin):
    def initialize(self, config: Dict[str, Any]):
        if "catalog" not in config:
            raise Exception("'catalog' is a required argument for the iceberg plugin!")
        catalog = config.pop("catalog")
        self._catalog = pyiceberg.catalog.load_catalog(catalog, **config)

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
