from pathlib import Path

import polars as pl

from jaeger_prometheus_joining.controlflow.ParseSettings import ParseSettings


class FileConcat:
    def __init__(self, settings: ParseSettings):
        self.settings = settings

    def start(self, output_path: Path, additional_name: str):
        df = pl.read_parquet(f"{output_path}/*")
        df.write_parquet(f"{output_path}/{additional_name}merged-file.parquet")
