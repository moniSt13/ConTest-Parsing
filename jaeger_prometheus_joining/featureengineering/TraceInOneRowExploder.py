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
import regex as re

from jaeger_prometheus_joining.controlflow.ParseSettings import ParseSettings


class TracesInOneRowExploder:
    def __init__(self, settings: ParseSettings):
        self.settings: ParseSettings = settings

    def start(self, source_path: Path, output_path: Path, source_path_systemwideMetrics: Path):
        df = pl.read_csv(source_path)
        df = self.typecast_column_if_exists(df, pl.Utf8, "http.status_code")
        one_line_dfs, microservice_lookup_df = self.__split_trace_into_one_row(df)
        final_df = self.__combine_single_traces(one_line_dfs, microservice_lookup_df)
        df_systemwideMetrics = pl.read_csv(source_path_systemwideMetrics)
        final_df = self.__add_SystemWideMetrics(final_df, df_systemwideMetrics)
        
        self.__write_to_disk(final_df, output_path)

    def __split_trace_into_one_row(
        self, df: pl.DataFrame
    ) -> tuple[list[pl.DataFrame], pl.DataFrame]:
        microservice_lookup = {}
        all_one_line_traces = []
        df = self.typecast_column_if_exists(df, pl.Utf8, "http.status_code")
        
        grouped_by_trace = df.group_by("traceID")
        
        list_of_metrics_float32 = [
            "container_cpu_usage_seconds_total", #
            "container_memory_working_set_bytes", #
            "container_cpu_user_seconds_total", #
            "container_cpu_system_seconds_total", #
            "container_memory_usage_bytes", #
            "container_threads" #
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

            #print("traceid: ", trace_id, " trace_df: ", trace_df)
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
                col("container_threads").mean().alias("mean_container_threads"),
                col("container_threads").min().alias("min_container_threads"),
                col("container_threads").max().alias("max_container_threads"),
                col("http.status_code").mode().alias("mode_http_status_code"),


                #col("NumberofOccurances_all").max().alias("max_number_of_occurrences_ALL"),
                #col("NumberofOccurances_error").max().alias("max_number_of_occurrences_ERROR"),
                #col("NumberofOccurances_info").max().alias("max_number_of_occurrences_INFO"),
                #col("NumberofOccurances_warn").max().alias("max_number_of_occurrences_WARN"),
            )
            
            
            one_row_traces = []
            for single_service_json in aggregated_df.iter_rows(named=True):
                #print("single_service_json: ", single_service_json)
                
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

    def __add_SystemWideMetrics(self, df_with_singleline_traces: pl.DataFrame, df_with_system_wide_metrics: pl.DataFrame) -> pl.DataFrame:
        print(df_with_system_wide_metrics.glimpse())
        #rename column
        #df_with_system_wide_metrics = df_with_system_wide_metrics.rename({
        #    "measure_time": "starttime"
        #})
        # join for all columns with start times in df_with_singleline_traces
        starttime_columns = [col for col in df_with_singleline_traces.columns if "starttime" in col]
        print("starttime_columns : ", starttime_columns)
        
        #for counter in range(len(starttime_columns)):
        counter = 0
        for microservice_starttime in starttime_columns:
            
            print("COUNTER FOR JOIN: ", counter)
            df_with_singleline_traces = df_with_singleline_traces.join(df_with_system_wide_metrics, left_on=microservice_starttime, right_on="systemWide-measure_time", how="left", suffix=str(counter))
            counter += 1
        df_with_singleline_traces.write_csv("df_with_singleline_traces.csv")

        #get column names that need merging
        tmp_list_columnnames = []
        for ii in range(counter):
            tmp_list_columnnames.append([col for col in df_with_singleline_traces.columns if str(ii) in col])

        tmp_list_columnnames.pop(0)
        
        #transpose list
        tmp_list_columnames_transposed = list(map(list, zip(*tmp_list_columnnames)))#[list(row) for row in zip(*tmp_list_columnnames)]  # list(map(list, zip(*tmp_list_columnames)))
        #add element with no number to the transposed list
        for index,row in enumerate(tmp_list_columnames_transposed):
            pattern = r'[0-9]'
            newElement = re.sub(pattern, '', row[0])
            tmp_list_columnames_transposed[index].insert(0, newElement)
            
              

        #merge together joined values in tmp_list_columnames_transposed as list of elements (e.g.; <list[f64]> [39444206008.190475, None, None])
        for row in tmp_list_columnames_transposed:
            df_with_singleline_traces = df_with_singleline_traces.with_columns(df_with_singleline_traces.select(
                pl.concat_list(pl.selectors.starts_with(x)).alias(x) for x in row#columnsToChange
            ))

        #select only a non-None value to be kept in the df of systemWide metrics
        
        first_items_from_tmp_list_columnames_transposed = [item[0] for item in tmp_list_columnames_transposed]
        print("first items in transposed columnsnames list: ", first_items_from_tmp_list_columnames_transposed)
        for firstElementInRow in first_items_from_tmp_list_columnames_transposed:
            tmp_list_for_s = []
            for n in range(len(df_with_singleline_traces[firstElementInRow])):
                for list_length in range(len(df_with_singleline_traces[firstElementInRow][n])):
                    if df_with_singleline_traces[firstElementInRow][n][list_length] != None:
                        tmp = df_with_singleline_traces[firstElementInRow][n][list_length]
                        #print("df apple_run_list_length:", firstElementInRow, "    :", df_with_singleline_traces[firstElementInRow][n][list_length])
                        tmp_list_for_s.append(tmp)
            s = pl.Series(firstElementInRow+"_final", tmp_list_for_s)
            print("tmp_list_for_first element in row:", firstElementInRow, "and Series: ", s)
            df_with_singleline_traces = df_with_singleline_traces.hstack([s])  
        #delete rows in that include columns_to_change
        for rows in tmp_list_columnames_transposed:
            for columnsToChange in rows:
                df_with_singleline_traces = df_with_singleline_traces.drop(columnsToChange)

        print("Concatenated df_with_singleline_traces: ", df_with_singleline_traces.glimpse())
        return df_with_singleline_traces

    
    


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
