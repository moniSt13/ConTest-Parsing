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
        logs_filepath: list[Path],
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
        
        #df_joined = self.__join_with_logs(df_joined, df_logs)
        #join logs based on timestamp and servicename instead of joining based on spanID (join_with_logs)
        df_joined = self.__join_with_logs_timeframe(df_joined, df_logs)
        
        self.__write_to_disk(df_joined, output_path)

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
        log_df.write_csv("log_df.csv")
        joined_df = df.join(
            log_df,
            left_on=["servicename", "starttime"],
            right_on=["source-servicename", "timestamp"],
            how="left",
        )
        
        

        #create dict of available spans and their eventId
        available_spans = joined_df.select("spanID").to_dict()

        #count occurances of columen Level per SpanID and add column to df
        #joined_df = joined_df.with_columns([when(col("Level") == "WARN").then(1).otherwise(0).alias("warnings")])
        list_of_warnings = {
            'spanID': [],
            'AmountOfWarning': [],
            'AmountOfError': [],
            'AmountOfInfo': [],
            'amount of uniue eventID per spanID': []
        }
        
        for spanID in available_spans['spanID']:
            warnings_per_span = joined_df.filter(col("spanID") == spanID).filter(col("Level") == "WARN").height
            error_per_span = joined_df.filter(col("spanID") == spanID).filter(col("Level") == "ERROR").height
            info_per_span = joined_df.filter(col("spanID") == spanID).filter(col("Level") == "INFO").height
            count_unique_eventID = joined_df.filter(col("spanID") == spanID).select("EventId").unique().height
            list_of_warnings['spanID'].append(spanID)
            list_of_warnings['AmountOfWarning'].append(warnings_per_span)
            list_of_warnings['AmountOfError'].append(error_per_span)
            list_of_warnings['AmountOfInfo'].append(info_per_span)
            list_of_warnings['amount of uniue eventID per spanID'].append(count_unique_eventID)

        
        df_temp = pl.from_dict(list_of_warnings)
        #remove dublicates from df_temp["spanID"]
        df_temp = df_temp.unique("spanID")

                
        joined_df = joined_df.join(
            df_temp,
            on=["spanID"],
            how="left",
        )

        #joined_df = joined_df.with_columns([when(col("spanID") == spanID).then(warnings_per_span).otherwise(0).alias("warnings")])

        #    print("warnings per span: ", warnings_per_span, "with spanID: ", spanID, "with eventID", joined_df.filter(col("spanID") == spanID).select("EventId"))
        #    #write warnings_per_span to joined_df
     
        #    print("joined df spanID", joined_df.filter(col("spanID") == spanID))
        #    joined_df = joined_df.with_columns([when(col("spanID") == spanID).then(warnings_per_span).otherwise(0).alias("warnings")])

        joined_df.write_csv("joined.csv")


        #print("available spans: ", available_spans.values())
        #for uniquespanId in available_spans['spanID']:
        #    print("spanId: ", uniquespanId)
            # select eventId for each spanId and add to available_spans            
            #eventId = joined_df.filter(col("spanID") == spanId).select("EventId").to_dict()
            #available_spans[spanId] = "test für spanId" + spanId
        
        
        

         
        #added_eventIds = joined_df.select("EventId").to_dict()
        # ad eventIds to available_spans


        #available_spans = {joined_df.filter(col("spanID") == spanID).select("EventId").to_dict() for spanID in available_spans}
        #print("available spans and corresponding eventIds: ", available_spans)
        
        #create list of available logs
        #available_logs = joined_df.select("EventId").to_list()


        # TODO count unique spanIDs (how many joins happened) and add count to row. drop duplicate spanId rows
        n_logs_per_span = joined_df.select(pl.col("spanID").value_counts()).unnest(
            "spanID"
        )
        #print("logs joining: n_logs_per_span", n_logs_per_span)

        joined_df = (
            joined_df.unique("spanID")
            .join(n_logs_per_span, on="spanID")
            .with_columns([when(col("EventId").is_null()).then(0).otherwise(col("count")).alias("log-count")])
            .drop("Level", "EventId", "original_timestamp", "count")
        )

        # TODO works as intended, already tested with output
        # print(n_logs_per_span)

        return joined_df

    #counts number of occurances of a log per service until a specific timeframe
    def __join_with_logs_timeframe(self, df: pl.DataFrame, log_df: pl.DataFrame):
        
        list_of_occurances = {
            'starttime': [],
            'servicename': [],
            'NumberofOccurances_all': [],
            'NumberofOccurances_warn': [],
            'NumberofOccurances_error': [],
            'NumberofOccurances_info': [],
            'numberOfUniqueEventIds': [],
        }
        list_times = df.get_column('starttime').unique()

        microservices = log_df.get_column('source-servicename').unique()
        
        for date in df.get_column('starttime').unique():
            for microservice in log_df.get_column('source-servicename').unique():
                list_of_occurances['servicename'].append(microservice)
                list_of_occurances['starttime'].append(date)
                #filter df that is smaller than date and grouped by microservice
                list_of_occurances['NumberofOccurances_all'].append(log_df.filter(col('original_timestamp') <= date).filter(col('source-servicename') == microservice).height)
                list_of_occurances['NumberofOccurances_warn'].append(log_df.filter(col('original_timestamp') <= date).filter(col('Level') == "WARN").filter(col('source-servicename') == microservice).height)
                list_of_occurances['NumberofOccurances_error'].append(log_df.filter(col('original_timestamp') <= date).filter(col('Level') == "ERROR").filter(col('source-servicename') == microservice).height)
                list_of_occurances['NumberofOccurances_info'].append(log_df.filter(col('original_timestamp') <= date).filter(col('Level') == "INFO").filter(col('source-servicename') == microservice).height)
                list_of_occurances['numberOfUniqueEventIds'].append(log_df.filter(col('original_timestamp') <= date).filter(col('source-servicename') == microservice).select("EventId").unique().height)        
        # for whole system -> microservice unspecific
        #list_of_occurances['NumberofOccurances_all'].append(log_df.filter(col('original_timestamp') <= date).height)
        #iterate of log_df['original_timestamp'] and count how many logs are available until date
        df_temp = pl.from_dict(list_of_occurances)
        


        joined_df = df.join(
            df_temp,
            on=["starttime",'servicename'],
            how="left",
        )
        #joined_df.write_csv("joined_with_logs_timeframe.csv")
        return joined_df

    def __write_to_disk(self, df: pl.DataFrame, output_path: Path):
        if self.settings.save_to_disk:
            df.write_csv(output_path)
