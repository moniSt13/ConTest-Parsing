from pathlib import Path

from jaeger_prometheus_joining.controlflow.ParseSettings import ParseSettings
import polars as pl


class FileConcat:
    def __init__(self, settings: ParseSettings):
        self.settings = settings

    def start(self, output_path: Path, additional_name: str):
        df = pl.read_parquet(f"{output_path}/*")
        df.write_parquet(f"{output_path}/{additional_name}merged-file.parquet")
