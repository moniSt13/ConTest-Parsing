import os
from pathlib import Path

import polars as pl
from logparser.AEL import LogParser

from jaeger_prometheus_joining.controlflow.ParseSettings import ParseSettings


class LogsParser:
    def __init__(self, settings: ParseSettings):
        self.settings: ParseSettings = settings

    def start(self, source_path: Path, output_path: Path):
        self.__parameterize_logs(source_path, output_path)
        self.__to_joinable_format(output_path)
        # df = self.__parse_data(source_path)
        # self.__write_to_disk(df, output_path)

    def __parameterize_logs(self, source_path: Path, output_path: Path):
        input_dir = source_path.parent  # The input directory of log file
        output_dir = output_path  # The output directory of parsing results
        log_file = source_path.name  # The input log file name
        # log_format = '<Date> <Time> <Level> --- <Origin>:<Content>'
        log_format = "<Date> <Time> <Level> <Number>---<LoggingReporter>: <Content>"
        minEventCount = 2  # The minimum number of events in a bin
        merge_percent = 0.5  # The percentage of different tokens

        parser = LogParser(
            input_dir,
            output_dir,
            log_format,
            minEventCount=minEventCount,
            merge_percent=merge_percent,
        )
        parser.parse(log_file)

    def __to_joinable_format(self, output_path: Path):
        for logfile in output_path.iterdir():
            if 'structured' not in logfile.name:
                continue

            df = (pl.read_csv(logfile)
                  .drop("Content", "EventTemplate", "ParameterList", "LineId", "Number")
                  .with_columns([
                        ("ts-" + pl.col("LoggingReporter").str.split("] ").list.get(1).str.split(".").list.get(0) + "-service").alias("source-servicename"),
                        (pl.col("Date") + " " + pl.col("Time")).str.to_datetime().dt.round(self.settings.rounding_acc).alias("timestamp"),
                        (pl.col("Date") + " " + pl.col("Time")).str.to_datetime().alias("original_timestamp")
                  ])
                  .drop("LoggingReporter", "Date", "Time"))

            self.__write_to_disk(df, output_path.joinpath("LOGS_joinable.parquet"))


    def __write_to_disk(self, df: pl.DataFrame, output_path: Path):
        if not os.path.exists(output_path.parents[0]):
            os.makedirs(output_path.parents[0])

        df.write_parquet(output_path)
