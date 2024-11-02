import datetime
import json
from importlib import resources as impresources

from dagster import (
    DailyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
)

from dagster_pyiceberg_example import static

with (impresources.files(static) / "stations.json").open("r") as f:
    stations = [sd["number"] for sd in json.load(f)]

daily_partition = DailyPartitionsDefinition(
    start_date=datetime.datetime(2024, 10, 20),
    end_offset=0,
    timezone="Europe/Amsterdam",
    fmt="%Y-%m-%d",
)

stations_partition = StaticPartitionsDefinition(stations)

daily_station_partition = MultiPartitionsDefinition(
    {
        "daily": daily_partition,
        "stations": stations_partition,
    }
)
