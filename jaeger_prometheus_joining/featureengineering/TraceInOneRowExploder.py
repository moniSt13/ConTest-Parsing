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

to

| 0-traceID | 0-spanID | ts-admin | 1-traceID | 1-spanID | ts-seat | 2-traceID | 2-spanID | ts-security | 3-traceID | 3-spanID | ts-another |
|-----------|----------|----------|-----------|----------|---------|-----------|----------|-------------|-----------|----------|------------|
| 1         | 1        | true     | 1         | 2        | true    | 1         | 4        | true        | -         | -        | -          |
| 1         | 3        | true     | 1         | 2        | true    | 1         | 4        | true        | -         | -        | -          |


"""

import os
from pathlib import Path

import polars as pl
from polars import col, Utf8

from jaeger_prometheus_joining.controlflow.ParseSettings import ParseSettings


class TracesInOneRowExploder:
    def __init__(self, settings: ParseSettings):
        self.settings: ParseSettings = settings

    def start(self, source_path: Path, output_path: Path):
        df = pl.read_csv(source_path)
        df = self.typecast_column_if_exists(df, pl.Utf8, "http.status_code")
        one_line_dfs, microservice_lookup_df = self.__split_trace_into_one_row(df)
        print("was braucht hier so lange: split trace into one row")
        final_df = self.__combine_single_traces(one_line_dfs, microservice_lookup_df)
        print("was braucht hier so lange: combine single traces")
        self.__write_to_disk(final_df, output_path)

    def __split_trace_into_one_row(
        self, df: pl.DataFrame
    ) -> tuple[list[pl.DataFrame], pl.DataFrame]:
        microservice_lookup = {}
        all_one_line_traces = []
        df = self.typecast_column_if_exists(df, pl.Utf8, "http.status_code")
        #print("df: ", df)
        #print("After typecast of http status code")
        grouped_by_trace = df.group_by("traceID")
        #print("grouped by traceID", grouped_by_trace)
        list_of_metrics_float32 = [
            "container_cpu_usage_seconds_total", #
            "container_memory_working_set_bytes", #
            "container_cpu_user_seconds_total", #
            "container_cpu_system_seconds_total", #
            "container_memory_usage_bytes" #
        ]

        # Split df into groups of their traces
        for trace_id, trace_df in grouped_by_trace:  # type: str, pl.DataFrame
            # Calculate statistics and averages for one trace
            trace_duration = trace_df.select(col("duration").sum()).item()
            trace_span_length = trace_df.height

            #add all potential metrices when they are empty
            for metric in list_of_metrics_float32:
                if metric not in trace_df.columns:
                    trace_df = trace_df.with_columns(
                        pl.lit(None, pl.Float32).alias(metric)
                    )
            
            '''if "container_cpu_usage_seconds_total" not in trace_df.columns:
                trace_df = trace_df.with_columns(
                    pl.lit(None, pl.Float32).alias("container_cpu_usage_seconds_total")
                )
            if "container_memory_working_set_bytes" not in trace_df.columns:
                trace_df = trace_df.with_columns(
                    pl.lit(None, pl.Float32).alias("container_memory_working_set_bytes")
                )
            if "container_cpu_user_seconds_total" not in trace_df.columns:
                trace_df = trace_df.with_columns(
                    pl.lit(None, pl.Float32).alias("container_cpu_user_seconds_total")
                )
            if "container_cpu_system_seconds_total" not in trace_df.columns:
                trace_df = trace_df.with_columns(
                    pl.lit(None, pl.Float32).alias("container_cpu_user_seconds_total")
                ) '''



            print("traceid: ", trace_id, " trace_df: ", trace_df)
            aggregated_df = trace_df.group_by(col("servicename")).agg(
                col("max_depth").mean().alias("mean_max_depth"),
                col("min_depth").mean().alias("mean_min_depth"),
                col("mean_depth").mean().alias("mean_mean_depth"),
                col("self_depth").mean().alias("mean_self_depth"),
                col("spanID", "operationName", "starttime"),
                pl.count().alias("spans_in_microservice"),
                col("container_cpu_usage_seconds_total")
                .mean()
                .alias("mean_container_cpu_usage_seconds_total"),
                col("container_memory_working_set_bytes")
                .mean()
                .alias("mean_container_memory_working_set_bytes"),
                col("container_cpu_usage_seconds_total")
                .max()
                .alias("max_container_cpu_usage_seconds_total"),
                col("container_memory_working_set_bytes")
                .max()
                .alias("max_container_memory_working_set_bytes"),
                col("container_cpu_usage_seconds_total")
                .min()
                .alias("min_container_cpu_usage_seconds_total"),
                col("container_memory_working_set_bytes")
                .min()
                .alias("min_container_memory_working_set_bytes"),
                col("container_cpu_user_seconds_total").max().alias("max_container_cpu_user_seconds_total"),
                col("container_cpu_user_seconds_total").min().alias("min_container_cpu_user_seconds_total"),
                col("container_cpu_user_seconds_total").mean().alias("mean_container_cpu_user_seconds_total"),
                col("container_cpu_system_seconds_total").max().alias("max_container_cpu_system_seconds_total"),
                col("container_cpu_system_seconds_total").min().alias("min_container_cpu_system_seconds_total"),
                col("container_cpu_system_seconds_total").mean().alias("mean_container_cpu_system_seconds_total"),
                col("container_memory_usage_bytes").max().alias("max_container_memory_usage_bytes"),
                col("container_memory_usage_bytes").min().alias("min_container_memory_usage_bytes"),
                col("container_memory_usage_bytes").mean().alias("mean_container_memory_usage_bytes"),
                col("duration").mean().alias("mean_duration"),
                col("duration").min().alias("min_duration"),
                col("duration").max().alias("max_duration"),
                col("NumberofOccurances_all").mean().alias("mean_number_of_occurrences_ALL"),
                col("NumberofOccurances_error").mean().alias("mean_number_of_occurrences_ERROR"),
                col("NumberofOccurances_info").mean().alias("mean_number_of_occurrences_INFO"),
                col("NumberofOccurances_warn").mean().alias("mean_number_of_occurrences_WARN"),
                col("numberOfUniqueEventIds").mean().alias("mean_number_of_unique_event_ids"),                                                                                     
                #col("NumberofOccurances_all").max().alias("max_number_of_occurrences_ALL"),
                #col("NumberofOccurances_error").max().alias("max_number_of_occurrences_ERROR"),
                #col("NumberofOccurances_info").max().alias("max_number_of_occurrences_INFO"),
                #col("NumberofOccurances_warn").max().alias("max_number_of_occurrences_WARN"),
            )
            
            print("Aggregated DF", aggregated_df)
            one_row_traces = []
            for single_service_json in aggregated_df.iter_rows(named=True):
                print("single_service_json: ", single_service_json)
                
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
                        pl.col("spanID").list.join(" - "),
                        pl.col("operationName").list.join(" - "),
                        pl.col("starttime").list.join(" - "),
                    ]
                )

                single_service_df = self.__i_dont_have_consistent_typing_and_it_sucks(
                    single_service_df.columns, single_service_df
                )
                

                single_service_df = single_service_df.rename(
                    self.__columns_with_prefix(
                        list(single_service_json.keys()),
                        servicename,
                    )
                )

                self.__hash_serviceentry_and_add(
                    single_service_df, servicename, microservice_lookup
                )

                one_row_traces.append(single_service_df)

            one_trace_df = pl.concat(one_row_traces, how="horizontal").with_columns(
                [
                    pl.lit(trace_id, pl.Utf8).alias("traceID"),
                    pl.lit(trace_span_length, pl.Int64).alias("traceLength"),
                ]
            )

            all_one_line_traces.append(one_trace_df)
        #all_one_line_traces.write_csv("all_one_line_traces.csv")
        if len(microservice_lookup.values()) == 0:
            return all_one_line_traces, pl.DataFrame()

        microservice_lookup_df = pl.concat(
            list(microservice_lookup.values()), how="horizontal"
        )
        print("all_one_line_traces: ", all_one_line_traces)
        print("microservice_lookup_df: ", microservice_lookup_df)
        return all_one_line_traces, microservice_lookup_df

    def __hash_serviceentry_and_add(
        self, df: pl.DataFrame, servicename: str, microservice_lookup: dict
    ):
        df = df.with_columns(
            [
                pl.lit(None, Utf8).alias(servicename + "-spanID"),
                pl.lit(None, Utf8).alias(servicename + "-starttime"),
                pl.lit(None, Utf8).alias(servicename + "-operationName"),
            ]
        )
        microservice_lookup[servicename] = df

    def __combine_single_traces(
        self, dfs: list[pl.DataFrame], microservice_lookup: pl.DataFrame
    ) -> pl.DataFrame:
        if len(dfs) > 0:
            concat_df = pl.concat(dfs, how="diagonal")
            concat_df_columns = concat_df.columns
            concat_df_columns.remove("traceLength")
            concat_df_columns.remove("traceID")
            concat_df = concat_df.select(
                col(name).fill_null(value=microservice_lookup.get_column(name))
                for name in concat_df_columns
            )
            return concat_df
        else:
            return pl.DataFrame()

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
            pl.Float32,
            "node_namespace_pod_container:container_memory_working_set_bytes",
        )
        df = self.typecast_column_if_exists(
            df, pl.Float32, "container_memory_mapped_file"
        )
        df = self.typecast_column_if_exists(
            df, pl.Float32, "node_namespace_pod_container:container_memory_rss"
        )
        df = self.typecast_column_if_exists(
            df, pl.Float32, "mean_container_memory_working_set_bytes"
        )
        df = self.typecast_column_if_exists(
            df, pl.Float32, "mean_container_cpu_usage_seconds_total"
        )
        df = self.typecast_column_if_exists(
            df, pl.Float32, "min_container_memory_working_set_bytes"
        )
        df = self.typecast_column_if_exists(
            df, pl.Float32, "min_container_cpu_usage_seconds_total"
        )
        df = self.typecast_column_if_exists(
            df, pl.Float32, "max_container_memory_working_set_bytes"
        )
        df = self.typecast_column_if_exists(
            df, pl.Float32, "max_container_cpu_usage_seconds_total"
        )
        df = self.typecast_column_if_exists(
            df,
            pl.Float32,
            "node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate",
        )
        df = self.typecast_column_if_exists(
            df, pl.Float32, "node_namespace_pod_container:container_memory_cache"
        )
        df = self.typecast_column_if_exists(df, pl.Float32, "prober_probe_total")
        df = self.typecast_column_if_exists(df, pl.Float32, "kube_pod_status_ready")
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
