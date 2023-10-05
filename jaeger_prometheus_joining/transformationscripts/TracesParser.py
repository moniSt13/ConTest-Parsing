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
        # pandas_df = pd.read_json(filepath)
        # pandas_df.drop(["total", "limit", "offset", "errors"], inplace=True, axis=1, errors="ignore")
        # pandas_df = pd.json_normalize(pandas_df["data"], "spans", ["processes"])
        #
        # if len(pandas_df) == 0:
        #     return
        #
        # pandas_df = pandas_df.drop("flags", axis=1)
        # pandas_df["processes"] = pandas_df["processes"].map(
        #     lambda x: [{k: v} for k, v in x.items()]
        # )
        # pandas_df["tags"] = pandas_df["tags"].map(
        #     lambda x: [
        #         {"key": str(entry["key"]), "value": str(entry["value"])} for entry in x
        #     ]
        # )

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

        # df = pl.from_pandas(pandas_df)
        #
        # df = df.drop("processes", "logs")
        # print(df.schema)
        return df, process_lookup

    def __transform_data(self, df: pl.DataFrame, process_lookup: dict):
        # if pl.List(pl.Null) not in df.dtypes:
        #     df = (
        #         df.rename({"spanID": "tempSpanID", "traceID": "tempTraceID"})
        #         .explode("references")
        #         .unnest("references")
        #         .rename(
        #             {
        #                 "tempSpanID": "spanID",
        #                 "spanID": "childSpanId",
        #                 "tempTraceID": "traceID",
        #                 "traceID": "childTraceId",
        #             }
        #         )
        #         .drop(["refType"])
        #     )
        # else:
        #     df = df.with_columns(
        #         [
        #             pl.lit("", pl.Utf8).alias("childSpanId"),
        #             pl.lit("", pl.Utf8).alias("childTraceId"),
        #         ]
        #     ).drop("references")
        #
        #         df = (
        #             df.explode("tags")
        #             .unnest("tags")
        # )
        #         )

        df = (
            df.explode("data")
            .unnest("data")
            .explode("spans")
            .unnest("spans")
            .explode("tags")
            .unnest("tags")
            .filter(col("key") == "http.status_code")
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

        # return df.select(
        #     [
        #         col("traceID"),
        #         col("spanID"),
        #         col("starttime"),
        #         col("operationName"),
        #         col("servicename"),
        #         col("podname"),
        #         col("childSpanId"),
        #         col("childTraceId"),
        #         col("duration"),
        #         col("http.status_code"),
        #     ]
        # )

    def __write_to_disk(self, df: pl.DataFrame, output_path):
        if self.settings.save_to_disk:
            if not os.path.exists(output_path.parents[0]):
                os.makedirs(output_path.parents[0])

            df.write_parquet(output_path)
