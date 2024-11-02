import os

from pyiceberg.catalog.sql import SqlCatalog

if __name__ == "__main__":
    catalog = SqlCatalog(
        name="dagster_example_catalog",
        **{
            "uri": os.environ["DAGSTER_SECRET_PYICEBERG_CATALOG_URI"],
            "s3.endpoint": os.environ["DAGSTER_SECRET_S3_ENDPOINT"],
            "s3.access-key-id": os.environ["DAGSTER_SECRET_S3_ACCESS_KEY_ID"],
            "s3.secret-access-key": os.environ["DAGSTER_SECRET_S3_SECRET_ACCESS_KEY"],
            "py-io-impl": "pyiceberg.io.fsspec.FsspecFileIO",
            "warehouse": os.environ["DAGSTER_SECRET_S3_WAREHOUSE"],
        },
    )
    catalog.create_namespace_if_not_exists("air_quality")
