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

    # DEPRECATED
    def __parse_data(self, source_path: Path) -> pl.DataFrame:
        with open(source_path) as logfile:
            result_dict = {
                "timestamp": [],
                "origin": [],
                "log_status": [],
                "error_message": [],
            }
            for line in logfile:
                if (
                    line.startswith("\t")
                    or line.isspace()
                    or not line[0].isdigit()
                    or line.find("---") == -1
                ):
                    pass
                else:
                    line_split_message_and_info = line.split(" : ")

                    error_message = line_split_message_and_info[-1].strip()
                    origin = line_split_message_and_info[0].split("] ")[1].strip()
                    timestamp = "T".join(
                        line_split_message_and_info[0].split("---")[0].split(" ")[0:2]
                    )
                    log_status = (
                        line_split_message_and_info[0]
                        .split("---")[0]
                        .strip()
                        .split(" ")[-2]
                        .strip()
                    )

                    result_dict["timestamp"].append(timestamp)
                    result_dict["origin"].append(origin)
                    result_dict["error_message"].append(error_message)
                    result_dict["log_status"].append(log_status)

        return pl.DataFrame(result_dict)

    def __write_to_disk(self, df: pl.DataFrame, output_path: Path):
        if not os.path.exists(output_path.parents[0]):
            os.makedirs(output_path.parents[0])

        df.write_parquet(output_path)
