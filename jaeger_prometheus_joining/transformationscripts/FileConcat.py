"""
This class reads every (parquet-)file in a folder and concatenates them into one single file.
"""
from pathlib import Path

import polars as pl

from jaeger_prometheus_joining.controlflow.ParseSettings import ParseSettings


class FileConcat:
    def __init__(self, settings: ParseSettings):
        self.settings: ParseSettings = settings

    def start(self, output_path: Path, additional_name: str):
        """
        :param output_path: Folderpath, in which all files are scanned and merged. Resulting file will be saved in the
        same folder with a suffix.
        :param additional_name: Suffix, with which the resulting file will be saved.
        :return: nothing
        """
        df = pl.read_parquet(f"{output_path}/*").unique(keep='none')
        df.write_parquet(f"{output_path}/{additional_name}merged-file.parquet")
