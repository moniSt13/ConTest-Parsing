"""
WIP

This isn't working and should be in feature engineering. This has really high resource consumption and multiprocessing
didn't help.

Implodes a trace into one line. Every spans is appended to the one-line-trace. The effect is that the row-count
decreases dramatically and the column count increases. Column counts can go over 1000 and is not static.

It may look like this:


| 0-traceID | 0-spanID | 0-microservice | 1-traceID | 1-spanID | 1-microservice | 2-traceID | 2-spanID | 2-microservice | 3-traceID | 3-spanID | 3-microservice |
|-----------|----------|----------------|-----------|----------|----------------|-----------|----------|----------------|-----------|----------|----------------|
| 1         | 1        | ts-admin       | 1         | 2        | ts-seat        | 1         | 3        | ts-admin       | 1         | 4        | ts-security    |
|           |          |                |           |          |                |           |          |                |           |          |                |

zu

| 0-traceID | 0-spanID | ts-admin | 1-traceID | 1-spanID | ts-seat | 2-traceID | 2-spanID | ts-security | 3-traceID | 3-spanID | ts-another |
|-----------|----------|----------|-----------|----------|---------|-----------|----------|-------------|-----------|----------|------------|
| 1         | 1        | true     | 1         | 2        | true    | 1         | 4        | true        | -         | -        | -          |
| 1         | 3        | true     | 1         | 2        | true    | 1         | 4        | true        | -         | -        | -          |


"""

import os
from pathlib import Path

import polars as pl
from polars import col

from jaeger_prometheus_joining.controlflow.ParseSettings import ParseSettings


class TracesInOneRowExploder:
    def __init__(self, settings: ParseSettings):
        self.settings: ParseSettings = settings

    def start(self, source_path: Path, output_path: Path):
        df = pl.read_csv(source_path)
        one_line_dfs = self.__split_trace_into_one_row(df)
        final_df = self.__combine_single_traces(one_line_dfs)
        self.__write_to_disk(final_df, output_path)

    def __split_trace_into_one_row(self, df: pl.DataFrame) -> list[pl.DataFrame]:
        all_one_line_traces = []
        grouped_by_trace = df.group_by("traceID")

        for trace_id, trace_df in grouped_by_trace:  # type: str, pl.DataFrame
            trace_duration = trace_df.select(col("duration").sum()).item()
            trace_span_length = trace_df.height
            # pseudo code: all metrics - fix values --> then aggregate

            if "container_cpu_usage_seconds_total" not in trace_df.columns:
                trace_df = trace_df.with_columns(pl.lit(None, pl.Int64).alias("container_cpu_usage_seconds_total"))
            if "container_memory_working_set_bytes" not in trace_df.columns:
                trace_df = trace_df.with_columns(pl.lit(None, pl.Int64).alias("container_memory_working_set_bytes"))



            aggregated_df = trace_df.group_by(col("servicename")).agg(
                col("max_depth").mean().alias("mean_max_depth"),
                col("min_depth").mean().alias("mean_min_depth"),
                col("mean_depth").mean().alias("mean_mean_depth"),
                col("self_depth").mean().alias("mean_self_depth"),
                col("spanID", "operationName", "starttime"),
                pl.count().alias("spans_in_microservice"),
                col("container_cpu_usage_seconds_total").mean().alias("mean_container_cpu_usage_seconds_total"),
                col("container_memory_working_set_bytes").mean().alias("mean_container_memory_working_set_bytes"),
                col("container_cpu_usage_seconds_total").max().alias("max_container_cpu_usage_seconds_total"),
                col("container_memory_working_set_bytes").max().alias("max_container_memory_working_set_bytes"),
                col("container_cpu_usage_seconds_total").min().alias("min_container_cpu_usage_seconds_total"),
                col("container_memory_working_set_bytes").min().alias("min_container_memory_working_set_bytes"),
                col("duration").mean().alias("mean_duration"),
                col("duration").min().alias("min_duration"),
                col("duration").max().alias("max_duration"),


            )
            # container_cpu_usage_seconds_total
            # container_memory_working_set_bytes

            one_row_traces = []
            for single_service_json in aggregated_df.iter_rows(named=True):
                single_service_json["spanID"] = [single_service_json["spanID"]]
                single_service_json["starttime"] = [single_service_json["starttime"]]
                single_service_json["operationName"] = [
                    single_service_json["operationName"]
                ]
                servicename = single_service_json["servicename"]
                single_service_json.pop("servicename")
                # print(single_service_json)
                single_service_df = (
                    pl.DataFrame(
                        single_service_json,
                    )
                ).with_columns(
                    [
                        pl.col("spanID").list.join("; "),
                        pl.col("operationName").list.join("; "),
                        pl.col("starttime").list.join("; "),
                    ]
                )

                single_service_df = single_service_df.rename(
                    self.__columns_with_prefix(
                        list(single_service_json.keys()),
                        servicename,
                    )
                )
                # print(single_service_df)

                single_service_df = self.__i_dont_have_consistent_typing_and_it_sucks(
                    single_service_df.columns, single_service_df
                )
                # print(single_service_df)
                one_row_traces.append(single_service_df)

            one_trace_df = pl.concat(one_row_traces, how="horizontal").with_columns(
                [
                    pl.lit(trace_id, pl.Utf8).alias("traceID"),
                    pl.lit(trace_span_length, pl.Int64).alias("traceLength"),
                ]
            )

            all_one_line_traces.append(one_trace_df)

        return all_one_line_traces

    def __combine_single_traces(self, dfs: list[pl.DataFrame]) -> pl.DataFrame:
        if len(dfs) > 0:
            return pl.concat(dfs, how="diagonal")
        else:
            raise Exception("combinatorics didnt work")

    def __write_to_disk(self, df: pl.DataFrame, output_path):
        if self.settings.save_to_disk:
            if not os.path.exists(output_path.parents[0]):
                os.makedirs(output_path.parents[0])

            df.write_csv(output_path)

    def __i_dont_have_consistent_typing_and_it_sucks(
        self, columns: list[str], df: pl.DataFrame
    ) -> pl.DataFrame:
        df = self.typecast_column_if_exists(
            df,
            pl.Utf8,
            "childSpanID",
        )
        df = self.typecast_column_if_exists(
            df,
            pl.Utf8,
            "childTraceID",
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
        df = self.typecast_column_if_exists(df, pl.Utf8, "http.status_code")

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
