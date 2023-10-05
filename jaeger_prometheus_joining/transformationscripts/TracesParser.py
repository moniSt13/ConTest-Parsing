import json
import os
from pathlib import Path

import polars as pl
from polars import col, Datetime, List, Struct, Field, Utf8, Int64

from jaeger_prometheus_joining.controlflow.ParseSettings import ParseSettings


class TracesParser:
    def __init__(self, settings: ParseSettings):
        self.settings = settings

    def start(self, source_path: Path, output_path: Path):
        df, process_lookup = self.__load_data(source_path)
        df = self.__transform_data(df, process_lookup)
        self.__write_to_disk(df, output_path)

        if self.settings.output_vis:
            print(source_path)
            print(df)
            print()

    def __load_data(self, filepath: Path):
        # Preprocessing lookup table for processes
        process_lookup = {}
        with open(filepath) as inp_file:
            raw_data = json.load(inp_file)["data"]
            processes = list(map(lambda x: x["processes"], raw_data))

            for row in processes:
                for processId in row:
                    servicename = row[processId]["serviceName"]
                    pod_name = ""
                    for tag in row[processId]["tags"]:
                        if tag["key"] == "hostname":
                            pod_name = tag["value"]
                    process_lookup[processId] = {
                        "servicename": servicename,
                        "podname": pod_name,
                    }

        schema = {
            "data": List(
                Struct(
                    [
                        Field("traceID", Utf8),
                        Field(
                            "spans",
                            List(
                                Struct(
                                    [
                                        Field("spanID", Utf8),
                                        Field("operationName", Utf8),
                                        Field("startTime", Int64),
                                        Field("duration", Int64),
                                        Field("processID", Utf8),
                                        Field(
                                            "tags",
                                            List(
                                                Struct(
                                                    [
                                                        Field("key", Utf8),
                                                        Field("value", Utf8),
                                                    ]
                                                )
                                            ),
                                        ),
                                        Field(
                                            "references",
                                            List(
                                                Struct(
                                                    [
                                                        Field("traceID", Utf8),
                                                        Field("spanID", Utf8),
                                                    ]
                                                )
                                            ),
                                        ),
                                    ]
                                )
                            ),
                        ),
                    ]
                )
            )
        }

        df = pl.read_json(filepath, schema=schema)

        return df, process_lookup

    def __transform_data(self, df: pl.DataFrame, process_lookup: dict):
        df = (
            df.explode("data")
            .unnest("data")
            .explode("spans")
            .unnest("spans")
            .explode("tags")
            .unnest("tags")
            .filter(
                (col("key") == "http.status_code") | (col("key") == "otel.status_code")
            )
            .drop("key")
            .rename({"value": "http.status_code"})
            .explode("references")
            .with_columns(
                [
                    col("references").struct.rename_fields(
                        ["childTraceID", "childSpanID"]
                    )
                ]
            )
            .unnest("references")
        )

        df = (
            df.with_columns(col("processID").map_dict(process_lookup))
            .unnest("processID")
            .rename({"startTime": "starttime"})
            .with_columns(
                col("starttime").cast(Datetime).dt.round(self.settings.rounding_acc)
            )
        )

        return df

    def __write_to_disk(self, df: pl.DataFrame, output_path):
        if self.settings.save_to_disk:
            if not os.path.exists(output_path.parents[0]):
                os.makedirs(output_path.parents[0])
            df.write_parquet(output_path)
