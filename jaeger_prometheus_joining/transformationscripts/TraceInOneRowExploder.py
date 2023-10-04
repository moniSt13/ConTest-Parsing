import os
from pathlib import Path

import polars as pl
from polars import col

from controlflow.ParseSettings import ParseSettings


class TracesInOneRowExploder:
    def __init__(self, settings: ParseSettings):
        self.settings = settings

    def start(self, source_path: Path, output_path: Path):
        df = pl.read_csv(source_path)
        one_line_dfs = self.__split_trace_into_one_row(df)
        final_df = self.__combine_single_traces(one_line_dfs)
        self.__write_to_disk(final_df, output_path)

    def __split_trace_into_one_row(self, df: pl.DataFrame) -> list[pl.DataFrame]:
        column_names = df.columns

        finished_one_line_traces = []
        grouped_by_trace = df.group_by("traceID")
        for trace_id, trace_df in grouped_by_trace:  # type: str, pl.DataFrame
            dfs_per_span = []
            dfs_per_span_same_column_name = list(
                map(lambda x: pl.DataFrame(x), trace_df.iter_rows(named=True))
            )
            for index, df_per_span in enumerate(
                dfs_per_span_same_column_name
            ):  # type: int, pl.DataFrame
                type_changed_df = self.__i_dont_have_consistent_typing_and_it_sucks(
                    column_names, df_per_span
                )
                rename = type_changed_df.rename(
                    self.__columns_with_prefix(column_names, str(index))
                )

                dfs_per_span.append(rename)

            # [print(dft.schema) for dft in dfs_per_span]
            exploded_trace = pl.concat(dfs_per_span, how="horizontal")
            finished_one_line_traces.append(exploded_trace)
        return finished_one_line_traces

    def __combine_single_traces(self, dfs: list[pl.DataFrame]) -> pl.DataFrame:
        return pl.concat(dfs, how="diagonal")

    def __write_to_disk(self, df: pl.DataFrame, output_path):
        if self.settings.save_to_disk:
            if not os.path.exists(output_path.parents[0]):
                os.makedirs(output_path.parents[0])

            df.write_parquet(output_path)

    def __i_dont_have_consistent_typing_and_it_sucks(
        self, columns: list[str], df: pl.DataFrame
    ) -> pl.DataFrame:
        df = df.with_columns(
            [
                col("childSpanId").cast(pl.Utf8).alias("childSpanId"),
                col("childTraceId").cast(pl.Utf8).alias("childTraceId"),
            ]
        )

        df = self.typecast_column_if_exists(
            df,
            pl.Float64,
            "node_namespace_pod_container:container_memory_working_set_bytes",
        )
        df = self.typecast_column_if_exists(
            df, pl.Float32, "container_memory_mapped_file"
        )
        df = self.typecast_column_if_exists(
            df, pl.Float64, "node_namespace_pod_container:container_memory_rss"
        )
        df = self.typecast_column_if_exists(
            df,
            pl.Float64,
            "node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate",
        )
        df = self.typecast_column_if_exists(
            df, pl.Float64, "node_namespace_pod_container:container_memory_cache"
        )
        df = self.typecast_column_if_exists(df, pl.Float64, "prober_probe_total")
        df = self.typecast_column_if_exists(df, pl.Float64, "kube_pod_status_ready")

        return df

    def typecast_column_if_exists(self, df: pl.DataFrame, type: type, column: str):
        if column in df.columns:
            return df.with_columns([col(column).cast(type).alias(column)])
        return df

    def __columns_with_prefix(
        self, column_names: list[str], prefix: str
    ) -> dict[str, str]:
        res = {}
        for column in column_names:
            res[column] = f"{prefix}-{column}"
        return res
