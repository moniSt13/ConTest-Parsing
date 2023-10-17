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

import multiprocessing
import os
import queue
from pathlib import Path

import polars as pl
from polars import col, Int64

from jaeger_prometheus_joining.controlflow.ParseSettings import ParseSettings
from jaeger_prometheus_joining.util.combinatorics import get_all_combinations


class TracesInOneRowExploder:
    def __init__(self, settings: ParseSettings):
        self.settings: ParseSettings = settings

    def start(self, source_path: Path, output_path: Path):
        df = pl.read_csv(source_path)
        one_line_dfs = self.__split_trace_into_one_row(df)
        final_df = self.__combine_single_traces(one_line_dfs)
        self.__write_to_disk(final_df, output_path)

    def __split_trace_into_one_row(self, df: pl.DataFrame) -> list[pl.DataFrame]:
        column_names = df.columns

        # jobs = []
        finished_one_line_traces = []
        grouped_by_trace = df.group_by("traceID")
        #
        # queue = multiprocessing.get_context("spawn").Queue()
        # queue.put(finished_one_line_traces)

        for trace_id, trace_df in grouped_by_trace:  # type: str, pl.DataFrame
            # print(trace_df.height)
            trace_duration = trace_df.select(col("duration").sum()).item()

            rows = list(trace_df.iter_rows(named=True))
            possible_row_combinations = get_all_combinations(rows, "servicename")
        #
            dfs_per_span = []

            for row in possible_row_combinations:
                dfs_per_combination = []
                for span in row: #type: dict
                    span[span['servicename']] = True
                    temp_df = (pl.DataFrame(span))

                    temp_df = self.__i_dont_have_consistent_typing_and_it_sucks(temp_df.columns, temp_df)
                    temp_df = temp_df.rename(
                        self.__columns_with_prefix(list(span.keys()), span['servicename'])
                    )
                    dfs_per_combination.append(temp_df)

                dfs_per_span.append(pl.concat(dfs_per_combination, how='horizontal')
                                    .with_columns([pl.lit(trace_duration, Int64).alias("trace_duration")]))
            finished_one_line_traces.extend(dfs_per_span)

        #     proc = multiprocessing.get_context("spawn").Process(target=self.intense_computing, args=(queue, rows, trace_duration))
        #     jobs.append(proc)
        #     proc.start()
        #
        # for fin_proc in jobs:
        #     fin_proc.join()

        # finished_one_line_traces = queue.get()
        print(len(finished_one_line_traces))
            # finished_one_line_traces.extend(intense_computing(rows))
        return finished_one_line_traces
                # print(x.select(col('traceID','spanID', 'servicename')))




            # dfs_per_span_same_column_name = list(
            #     map(lambda x: pl.DataFrame(x), trace_df.iter_rows(named=True))
            # )
            # for index, df_per_span in enumerate(
            #     dfs_per_span_same_column_name
            # ):  # type: int, pl.DataFrame
            #     type_changed_df = self.__i_dont_have_consistent_typing_and_it_sucks(
            #         column_names, df_per_span
            #     )
            #     rename = type_changed_df.rename(
            #         self.__columns_with_prefix(column_names, str(index))
            #     )
            #
            #     dfs_per_span.append(rename)

            # exploded_trace = (pl.concat(dfs_per_span, how="horizontal")
            #                     .with_columns([pl.lit(trace_duration, Int64).alias("trace_duration")]))
            # finished_one_line_traces.append(exploded_trace)


    # def intense_computing(self, queue, intense_rows, inner_trace_duration) -> list:
    #
    #     inner_finished_one_line_traces = queue.get(timeout=3)
    #
    #     possible_row_combinations = get_all_combinations(intense_rows, "servicename")
    #
    #     dfs_per_span = []
    #
    #     for row in possible_row_combinations:
    #         dfs_per_combination = []
    #         for span in row: #type: dict
    #             span[span['servicename']] = True
    #             temp_df = (pl.DataFrame(span))
    #
    #             temp_df = self.__i_dont_have_consistent_typing_and_it_sucks(temp_df.columns, temp_df)
    #             temp_df = temp_df.rename(
    #                 self.__columns_with_prefix(list(span.keys()), span['servicename'])
    #             )
    #             dfs_per_combination.append(temp_df)
    #
    #         dfs_per_span.append(pl.concat(dfs_per_combination, how='horizontal')
    #                             .with_columns([pl.lit(inner_trace_duration, Int64).alias("trace_duration")]))
    #     inner_finished_one_line_traces.extend(dfs_per_span)
    #     queue.put(inner_finished_one_line_traces)



    def __combine_single_traces(self, dfs: list[pl.DataFrame]) -> pl.DataFrame:
        if(len(dfs) > 0):
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
        df = df.with_columns(
            [
                col("childSpanID").cast(pl.Utf8),
                col("childTraceID").cast(pl.Utf8),
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
