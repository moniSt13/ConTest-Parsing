"""
Parses raw json-metric file. Can only parse a singular file and has no bulk option.
"""
import json
import os
from pathlib import Path

import pandas as pd
import polars as pl
from polars import last, col

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
        df = self.__filter_data(df, rename_name)
        self.__write_to_disk(df, output_path)

        if self.settings.output_vis:
            print(source_path)
            print(df)
            print()

    def __load_data(self, filepath: Path):
        with open(filepath) as json_src_file:
            rawdata = json.load(json_src_file)

        # because there is no inconsistent typing we can infer the schema
        rawdata = rawdata["data"]["result"]
        df = pd.DataFrame(rawdata)

        if len(df) == 0:
            return

        df["values"] = df["values"].transform(
            lambda x: [[float(i[0] * 1_000_000), float(i[1])] for i in x]
        )
        return pl.DataFrame(df)

    def __transform_data(self, df: pl.DataFrame):
        # we don't have every necessary column in every metric so we define standards to be able to vstack them
        df = df.unnest("metric")

        necessary_columns = [
            "__name__",
            "apiserver",
            "container",
            "instance",
            "method",
            "pod",
            "values",
        ]

        for i in necessary_columns:
            if i not in df.columns:
                df = df.with_columns([pl.lit(None, dtype=pl.Utf8).alias(i)])

        # we have to pivot the name of the metric and move it from the rows to a column, we would not be able to
        # efficiently vstack and join the data otherwise
        rename_name = df.head(1).to_dicts().pop()["__name__"]

        df = (
            df.select(necessary_columns)
            .explode("values")
            .with_columns(
                [
                    last()
                    .list[0]
                    .cast(pl.Datetime)
                    .dt.round(self.settings.rounding_acc)
                    .alias(f"measure_time"),
                    last().list[1].alias(rename_name),
                ]
            )
            .drop("__name__", "values")
        )

        return df, rename_name

    def __filter_data(self, df: pl.DataFrame, rename_name: str):
        if self.settings.drop_null:
            df = df.filter(col("container") != "POD").filter(col(rename_name) != 0)
        return df

    def __write_to_disk(self, df: pl.DataFrame, output_path: Path):
        if not os.path.exists(output_path.parents[0]):
            os.makedirs(output_path.parents[0])

        df.write_parquet(output_path)
