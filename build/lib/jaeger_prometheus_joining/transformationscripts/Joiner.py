from pathlib import Path

import polars as pl

from jaeger_prometheus_joining.controlflow.ParseSettings import ParseSettings


class Joiner:
    def __init__(self, settings: ParseSettings):
        self.settings = settings

    def start(
        self, tracing_filepath: Path, metrics_filepaths: list[Path], output_path: Path
    ):
        df_tracing, df_metrics = self.__load_data(tracing_filepath, metrics_filepaths)
        df_joined = self.__join_data(df_tracing, df_metrics)
        self.__write_to_disk(df_joined, output_path)

    def __load_data(self, tracing_filepath: Path, metrics_filepaths: list[Path]):
        tracing = (
            pl.read_parquet(tracing_filepath)
            .sort("starttime")
            .set_sorted("starttime")
            .unique("spanID")
        )

        all_metrics = []
        for metrics_filepath in metrics_filepaths:
            metric = pl.read_parquet(metrics_filepath)
            metric = metric.sort("measure_time").set_sorted("measure_time")
            all_metrics.append(metric)

        all_metrics.sort(key=lambda x: x.height, reverse=True)

        return tracing, all_metrics

    def __join_data(self, tracing: pl.DataFrame, all_metrics: list[pl.DataFrame]):
        for metrics in all_metrics:
            joined = tracing.join(
                metrics,
                left_on=["podname", "starttime"],
                right_on=["pod", "measure_time"],
                how="left",
            )
            joined = joined[:, [not (s.null_count() == joined.height) for s in joined]]

            if joined.height > 0:
                cur_height = joined.height
                joined = joined.unique("spanID")
                tracing = joined

                if self.settings.output_vis:
                    print(f"Datasize from {cur_height} to {cur_height - joined.height}")

            # Delete duplicate columns from right-table to prepare for next join
            duplicate_columns = [
                col_name for col_name in tracing.columns if col_name.endswith("right")
            ]
            if len(duplicate_columns) > 0:
                tracing = tracing.drop(duplicate_columns)

        if "container" in tracing.columns:
            tracing = tracing.drop_nulls(subset="container")

        return tracing

    def __write_to_disk(self, df: pl.DataFrame, output_path: Path):
        if self.settings.save_to_disk:
            df.write_csv(output_path)
