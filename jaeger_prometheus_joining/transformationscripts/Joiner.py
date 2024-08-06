"""This class is responsible for joining the traces and metrics based on timestamp and podname. The result is one
large csv-file."""

from pathlib import Path
from time import sleep

import polars as pl
from polars import when, col, lit
from datetime import timedelta

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
        output_path_systemWideMetrics: Path,
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
        print("log df: ", df_logs.glimpse())
        print("Before joining traces and metrics")
        df_joined = self.__join_data(df_tracing, df_metrics)
        print("After joining traces and metrics")
        #join logs based on timestamp and servicename instead of joining based on spanID (join_with_logs)
        print("Before joining logs")
        df_joined = self.__join_with_logs_timeframe(df_joined, df_logs)
        print("Before joining system wide metrics")
        df_joined = self.__add_systemWideNetworkMetrics(df_joined, df_metrics, output_path_systemWideMetrics)
       
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
            
            #metrics.glimpse()
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
        print("list_times: ", list_times)
        microservices = log_df.get_column('source-servicename').unique()
        print("microservices: ", microservices)
        #log_df['original_timestamp'] = log_df['original_timestamp'] - timedelta(hours=-2)

        for date in df.get_column('starttime').unique():
            
            #print("added date by an hour: ", date + timedelta(hours=2))
            #date = date + timedelta(hours=2)

            for microservice in log_df.get_column('source-servicename').unique():
                print("MICROSERVICE: ", microservice)
                list_of_occurances['servicename'].append(microservice)
                list_of_occurances['starttime'].append(date)
                #filter df that is smaller than date and grouped by microservice
                list_of_occurances['NumberofOccurances_all'].append(log_df.filter(col('original_timestamp') <= date).filter(col('source-servicename') == microservice).height)
                list_of_occurances['NumberofOccurances_warn'].append(log_df.filter(col('original_timestamp') <= date).filter(col('Level') == "WARN").filter(col('source-servicename') == microservice).height)
                list_of_occurances['NumberofOccurances_error'].append(log_df.filter(col('original_timestamp') <= date).filter(col('Level') == "ERROR").filter(col('source-servicename') == microservice).height)
                list_of_occurances['NumberofOccurances_info'].append(log_df.filter(col('original_timestamp') <= date).filter(col('Level') == "INFO").filter(col('source-servicename') == microservice).height)
                list_of_occurances['numberOfUniqueEventIds'].append(log_df.filter(col('original_timestamp') <= date).filter(col('source-servicename') == microservice).select("EventId").unique().height)        
        #print("list of occurances: ", list_of_occurances)
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
    



    def __join_data_systemSpecific(self, joined_df: pl.DataFrame, all_metrics: list[pl.DataFrame]):
        for metric in all_metrics:            
            if ("device") in metric.columns:
                #tmp_metric = metric.with_columns(
                #    pl.when(pl.col('device')=='eth0')
                #    .then(pl.lit('wholeSystem'))
                #    .otherwise(pl.lit('nothing'))
                #    .alias('new_device')
                #    )
                #print("TMP METRIC: ", tmp_metric.glimpse())

                #tmp_metric = metric.with_columns(pl.when(pl.col("device") == "eth0" & pl.col("job") == "kubernetes-pods").then(pl.lit("wholeSystem")))
                tmp_metric = metric.filter((col("device") == "eth0") & (col("job") == "kubernetes-pods"))
                if tmp_metric.height > 0:
                    joined_df = joined_df.join(
                        tmp_metric, 
                        left_on=["starttime"],
                        right_on=["measure_time"],
                        how="left",
                    )
                

                '''joined = joined_df[:, [not (s.null_count() == joined_df.height) for s in joined_df]]

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
                        tracing = tracing.drop(duplicate_columns) '''
                

        return joined_df

    def __add_systemWideNetworkMetrics(self, joined_df: pl.DataFrame, all_metrics: list[pl.DataFrame], output_path_systemWideMetrics: Path):
        #print(joined_df.glimpse())
        tmp_df: list[pl.DataFrame] = []
        for metric in all_metrics: 
            if ("device") in metric.columns:
                tmp_metric = metric.filter((col("device") == "eth0") & (col("job") == "kubernetes-service-endpoints"))
                if tmp_metric.height > 0:
                    tmp_metric = tmp_metric.drop("status", "resultType", "container", "endpoint", "id", "image", "instance", "job", "metrics_path", "name", "namespace", "node", "pod", "service", "net_host_name", "deployment", "apiserver", "method")
                    tmp_df.append(tmp_metric)
            #if ("subresource") in metric.columns:
            #    tmp_metric = metric.filter((col("subresource") == "/livez") & (col("job") == "kubernetes-apiservers")) #& (col("le") == "0.005")
            #    print("KOMME ICH HIER HIN mit: ", metric.glimpse())
            #    print("METRIC For theoretical subressource", tmp_metric)
            #    if tmp_metric.height > 0:
                   
            #        tmp_metric = tmp_metric.drop("status", "resultType", "container", "endpoint", "id", "image", "instance", "job", "metrics_path", "name", "namespace", "node", "pod", "service", "net_host_name", "deployment", "device", "method")
            #        tmp_df.append(tmp_metric)
              
        joined_df_metrics = pl.DataFrame({"original_date": tmp_df[0].get_column("original_date")})
        for i in range(len(tmp_df)):
            joined_df_metrics = pl.concat([joined_df_metrics, tmp_df[i]], how="align", rechunk=False, parallel=True)
        #remove duplicate columns
        joined_df_metrics = joined_df_metrics[:, [not (s.null_count() == joined_df_metrics.height) for s in joined_df_metrics]]
        joined_df_metrics = joined_df_metrics.rename({"device": "servicename"})#, "original_date": "original_timestamp", "measure_time": "starttime"})

        #add string to all columns
        for column in joined_df_metrics.columns:
            joined_df_metrics = joined_df_metrics.rename({column: "systemWide-"+column})
        
        joined_df_metrics = joined_df_metrics.drop("systemWide-servicename")
        
        #average of each column when systemWide-measure_time is the same (Original date is lost in this process)
        joined_df_metrics = joined_df_metrics.groupby("systemWide-measure_time").agg(
            [
                pl.col("systemWide-node_network_transmit_bytes_total").mean().alias("systemWide-node_network_transmit_bytes_total_mean"),
                pl.col("systemWide-node_network_transmit_drop_total").mean().alias("systemWide-node_network_transmit_drop_total_mean"),
                pl.col("systemWide-node_network_transmit_errs_total").mean().alias("systemWide-node_network_transmit_errs_total_mean"),
                pl.col("systemWide-node_network_transmit_packets_total").mean().alias("systemWide-node_network_transmit_packets_total_mean"),
                pl.col("systemWide-node_network_receive_bytes_total").mean().alias("systemWide-node_network_receive_bytes_total_mean"),
                #pl.col("systemWide-apiserver_request_duration_seconds_bucket").mean().alias("systemWide-apiserver_request_duration_seconds_bucket_mean"),
            ]
        )


        joined_df_metrics.write_csv(output_path_systemWideMetrics)
        
        

        #concat with joined_df
        joined_df = pl.concat([joined_df, joined_df_metrics], how="diagonal", rechunk=False, parallel=True)
        joined_df.write_csv("joined_df_all.csv")
        return joined_df


    def __write_to_disk(self, df: pl.DataFrame, output_path: Path):
        if self.settings.save_to_disk:
            df.write_csv(output_path)

