"""
Parses raw json-metric file. Can only parse a singular file and has no bulk option.
"""
import os
from pathlib import Path

import polars as pl
from polars import col, Utf8, Struct, Field, List, Float64

from jaeger_prometheus_joining.controlflow.ParseSettings import ParseSettings


class MetricsParser:
    def __init__(self, settings: ParseSettings):
        self.settings: ParseSettings = settings

    def start(self, source_path: Path, output_path: Path):
        """
        :param source_path: Filepath to the raw metric-json-file
        :param output_path: Filepath for the parsed file.
        :return: nothing
        """
        df = self.__load_data(source_path)
        df, rename_name = self.__transform_data(df)
        df = self.__cleanup_pause_containers(df)
        df = self.__filter_data(df, rename_name)
        df = self.__clean_no_defined_pod(df)
        self.__write_to_disk(df, output_path)

        if self.settings.output_vis:
            print(source_path)
            print(df)
            print()

    def __load_data(self, filepath: Path):
        schema = {
            "status": Utf8,
            "data": Struct(
                [
                    Field("resultType", Utf8),
                    Field(
                        "result",
                        List(
                            Struct(
                                [
                                    Field(
                                        "metric",
                                        Struct(
                                            [
                                                Field("__name__", Utf8),
                                                Field("container", Utf8),
                                                Field("device", Utf8),
                                                Field("endpoint", Utf8),
                                                Field("id", Utf8),
                                                Field("image", Utf8),
                                                Field("instance", Utf8),
                                                Field("job", Utf8),
                                                Field("metrics_path", Utf8),
                                                Field("name", Utf8),
                                                Field("namespace", Utf8),
                                                Field("node", Utf8),
                                                Field("pod", Utf8),
                                                Field("service", Utf8),
                                                #my identified fields that include microservice name
                                                Field("net_host_name", Utf8),
                                                Field("deployment", Utf8),
                                                Field("subresource", Utf8), #apiserver_request_duration_seconds_bucket
                                                Field("le", Utf8), #apiserver_request_duration_seconds_bucket                                                    
                                            ]
                                        ),
                                    ),
                                    Field("values", List(List(Utf8))),
                                ]
                            )
                        ),
                    ),
                ]
            ),
        }

        return pl.read_json(filepath, schema=schema)

    def __transform_data(self, df: pl.DataFrame):
        # we don't have every necessary column in every metric so we define standards to be able to vstack them
        # df = df.unnest("metric")

        df = (
            df.unnest("data")
            .explode("result")
            .unnest("result")
            .unnest("metric")
            .explode("values")
        )
        necessary_columns = [
            "__name__",
            "apiserver",
            "container",
            "instance",
            "method",
            "pod",
            "values",
            "device",
            "subresource",
            "le",
        ] #if mapping on other specify them here

        for i in necessary_columns:
            if i not in df.columns:
                df = df.with_columns([pl.lit(None, dtype=pl.Utf8).alias(i)])

        # we have to pivot the name of the metric and move it from the rows to a column, we would not be able to
        # efficiently vstack and join the data otherwise
        rename_name = df.head(1).to_dicts().pop()["__name__"]

        df = df.with_columns(
            [
                pl.from_epoch(col("values").list[0].cast(Float64)).alias("original_date"),                                                                          
                pl.from_epoch(col("values").list[0].cast(Float64)).dt.round(self.settings.rounding_acc).alias(
                    "measure_time"
                ),
                col("values").list[1].cast(Float64).alias(rename_name),
            ]
        ).drop("values", "__name__")

        
        return df, rename_name

    def __filter_data(self, df: pl.DataFrame, rename_name: str):
        if self.settings.drop_null:
            df = df.filter(col("container") != "POD").filter(col(rename_name) != 0)
        return df
    
    def __clean_no_defined_pod(self, df: pl.DataFrame): #clean rows that do not contain pod name or device because these two types are used for joining
        return df.filter(((col("pod") != "") | (col("pod").is_null() == False)) | (col("device").is_null() == False))

    def __cleanup_pause_containers(self, df: pl.DataFrame) -> pl.DataFrame:
        return df.with_columns([
            pl.when(col("container") == "POD").then(col("pod").str.split("-").list.reverse().list.slice(2).list.reverse().list.join("-")).otherwise(col("container")).alias("container"),
        ])

    def __write_to_disk(self, df: pl.DataFrame, output_path: Path):
        if not os.path.exists(output_path.parents[0]):
            os.makedirs(output_path.parents[0])

        df.write_parquet(output_path)
