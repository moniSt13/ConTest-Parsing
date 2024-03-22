"""This class is responsible for joining the traces and metrics based on timestamp and podname. The result is one
large csv-file."""

from pathlib import Path

import polars as pl
from polars import when, col, lit

from jaeger_prometheus_joining.controlflow.ParseSettings import ParseSettings


class Joiner:
    def __init__(self, settings: ParseSettings):
        self.settings: ParseSettings = settings

    def start(
        self,
        tracing_filepath: Path,
        metrics_filepaths: list[Path],
        logs_filepath: Path,
        output_path: Path,
    ):
        """
        :param logs_filepath:
        :param tracing_filepath: Filepath for the traces
        :param metrics_filepaths: Folderpath for all of the found metrics
        :param output_path: Outputpath where the merged file will be saved
        :return: nothing
        """
        df_tracing, df_logs, df_metrics = self.__load_data(
            tracing_filepath, logs_filepath, metrics_filepaths
        )

        df_joined = self.__join_data(df_tracing, df_metrics)
        
        df_joined = self.__join_with_logs(df_joined, df_logs)
        
        self.__write_to_disk(df_joined, output_path)
        print("loaded to disk")

    def __load_data(
        self, tracing_filepath: Path, logs_filepath: Path, metrics_filepaths: list[Path]
    ):
        """
        We load our concatenated trace-file and every metric we could parsed. We sort them based on the timestamp and
        filter out duplicate spanIDs (this should be done with the settings.drop_null variable and not a hardcoded
        default! wip). Sorting helps with joining the data more efficiently. The different metrics are sorted by
        their height, because depending on join-technique this can have impact on the resulting data size we receive.
        We cannot guarantee that we have a result for every span!
        :param tracing_filepath:
        :param metrics_filepaths:
        :return:
        """

        tracing = (
            pl.read_parquet(tracing_filepath).sort("starttime").set_sorted("starttime")
        )

        logs = pl.read_parquet(logs_filepath).sort("timestamp").set_sorted("timestamp")

        all_metrics = []
        for metrics_filepath in metrics_filepaths:
            metric = pl.read_parquet(metrics_filepath)
            metric = metric.sort("measure_time").set_sorted("measure_time")
            all_metrics.append(metric)

        all_metrics.sort(key=lambda x: x.height, reverse=True)

        return tracing, logs, all_metrics

    def __join_data(self, tracing: pl.DataFrame, all_metrics: list[pl.DataFrame]):
        """
        We join data with a left join, where the left table are the traces. Keys are podname and timestamp. We have
        to drop duplicate columns, which were joined because of additional information in the parsed metrics (could
        be improved for performance gains).

        Several strategies were tested: Left join, inner join, outer join but this method has resulted in the most data
        in a consistent matter.
        :param tracing:
        :param all_metrics:
        :return:
        """
        for metrics in all_metrics:
            joined = tracing.join(
                metrics,
                left_on=["podname", "starttime"],
                right_on=["pod", "measure_time"],
                how="left",
            )

            # Resource hungry
            # joined = tracing.join_asof(
            #     metrics,
            #     left_on="starttime",
            #     right_on="measure_time",
            #     by_left="podname",
            #     by_right="pod",
            # )
            # print(joined.select(pl.last()))

            joined = joined[:, [not (s.null_count() == joined.height) for s in joined]]

            if joined.height > 0:
                cur_height = joined.height
                joined = joined.unique("spanID")
                tracing = joined

                if self.settings.output_vis:
                    print(f"Datasize from {cur_height} to {cur_height - joined.height}")

            duplicate_columns = [
                col_name for col_name in tracing.columns if col_name.endswith("right")
            ]
            if len(duplicate_columns) > 0:
                tracing = tracing.drop(duplicate_columns)

        # Drops all data which couldn't be joined
        if "container" in tracing.columns:
            tracing = tracing.drop_nulls(subset="container")

        return tracing

    def __join_with_logs(self, df: pl.DataFrame, log_df: pl.DataFrame):
        joined_df = df.join(
            log_df,
            left_on=["servicename", "starttime"],
            right_on=["source-servicename", "timestamp"],
            how="left",
        )

        # TODO count unique spanIDs (how many joins happened) and add count to row. drop duplicate spanId rows
        n_logs_per_span = joined_df.select(pl.col("spanID").value_counts()).unnest(
            "spanID"
        )

        joined_df = (
            joined_df.unique("spanID")
            .join(n_logs_per_span, on="spanID")
            .with_columns([when(col("EventId").is_null()).then(0).otherwise(col("count")).alias("log-count")])
            .drop("Level", "EventId", "original_timestamp", "count")
        )

        # TODO works as intended, already tested with output
        # print(n_logs_per_span)

        return joined_df

    def __write_to_disk(self, df: pl.DataFrame, output_path: Path):
        if self.settings.save_to_disk:
            df.write_csv(output_path)
