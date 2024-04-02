"""Parses raw json-traces file. Can only parse a singular file and has no bulk option. After all traces have been
parsed the `FileConcat` will concatenate them together."""
import json
import os
from pathlib import Path

import polars as pl
from polars import col, when, Datetime, List, Struct, Field, Utf8, Int64, lit

from jaeger_prometheus_joining.controlflow.ParseSettings import ParseSettings


class TracesParser:
    def __init__(self, settings: ParseSettings):
        self.settings: ParseSettings = settings

    def start(self, source_path: Path, output_path: Path):
        """
        :param source_path: Filepath to the raw metric-traces-file
        :param output_path: Filepath for the parsed file.
        :return: nothing
        """
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

        # schema definition takes care 90% of parsing
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
        # we parsed nested json fields as polars-structs and explode them here
        # furthermore we parse ONLY the http statuscode and ignore the rest, not used fields are dropped
        df = (
            df.explode("data")
            .unnest("data")
            .explode("spans")
            .unnest("spans")
            .explode("tags")
            .unnest("tags")
            .with_columns(
                [
                    when(
                        (col("key") == "http.status_code")
                        | (col("key") == "otel.status_code")
                    )
                    .then(col("value").cast(Utf8))
                    .otherwise(lit(None))
                    .alias("http.status_code")
                ]
            )
            .drop(["key", "value"])
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

        # map the processID to our lookup table and round the timestamp
        df = (
            df.with_columns(col("processID").map_dict(process_lookup))
            .unnest("processID")
            .rename({"startTime": "starttime"})
            .with_columns(
                col("starttime").cast(Datetime).dt.round(self.settings.rounding_acc)
            )
             .with_columns(
                [
                    col("starttime").cast(Datetime).alias("original_timestamp")
                ]
            )              
        )

        return df

    def __write_to_disk(self, df: pl.DataFrame, output_path):
        if self.settings.save_to_disk:
            if not os.path.exists(output_path.parents[0]):
                os.makedirs(output_path.parents[0])
            df.write_parquet(output_path)
