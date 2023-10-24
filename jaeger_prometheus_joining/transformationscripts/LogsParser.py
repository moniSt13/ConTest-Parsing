import os
from pathlib import Path
import polars as pl

from jaeger_prometheus_joining.controlflow.ParseSettings import ParseSettings


class LogsParser:
    def __init__(self, settings: ParseSettings):
        self.settings: ParseSettings = settings

    def start(self, source_path: Path, output_path: Path):
        df = self.__parse_data(source_path)
        self.__write_to_disk(df, output_path)

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
