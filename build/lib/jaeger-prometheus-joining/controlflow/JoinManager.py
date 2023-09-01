import shutil
from pathlib import Path

from controlflow.ParseSettings import ParseSettings
from transformationscripts.FileConcat import FileConcat
from transformationscripts.FilepathFinder import FilepathFinder
from transformationscripts.Joiner import Joiner
from transformationscripts.MetricsParser import MetricsParser
from transformationscripts.TracesParser import TracesParser
from util.timedecorator import timer


class JoinManager:
    def __init__(self):
        self.settings = ParseSettings()

    def process(self):
        path_list = FilepathFinder(self.settings).find_files()

        stats = '\n'.join([f"{k}: {v}" for k, v in self.__calculate_statistics(path_list).items()])

        if self.settings.print_statistics:
            print(
                f"\n\n\n___ Following settings found: \n"
                f"{self.settings}\n\n"
                f"___ Found following folders to process: \n"
                f"{stats}\n\n"
                f"___ Processing time: \n"
            )

        self.__clear_output()
        self.__parse_metrics(path_list)
        self.__parse_traces(path_list)
        self.__join()

    @timer
    def __clear_output(self):
        if not self.settings.clear_output:
            return

        if self.settings.out.exists() and self.settings.out.is_dir():
            shutil.rmtree(self.settings.out)

    @timer
    def __parse_metrics(self, path_list: dict):
        metrics_parser = MetricsParser(self.settings)

        for service, sources in path_list.items():
            for monitoring_file in sources["monitoring"]:
                filename = monitoring_file.name.lower().split(".")[-2] + ".parquet"
                output_path = self.settings.out.joinpath(
                    service, "monitoring", filename
                )

                metrics_parser.start(monitoring_file, output_path)

    @timer
    def __parse_traces(self, path_list: dict):
        traces_parser = TracesParser(self.settings)
        for service, sources in path_list.items():
            for trace_file in sources["traces"]:
                filename = trace_file.name.lower().split(".")[-2] + ".parquet"
                output_path = self.settings.out.joinpath(service, "traces", filename)

                traces_parser.start(trace_file, output_path)

            self.__concat_files(
                self.settings.out.joinpath(service, "traces"),
                self.settings.additional_name_tracing,
            )

    @timer
    def __join(self):
        joiner = Joiner(self.settings)

        for service in self.settings.out.iterdir():
            if not service.is_dir():
                continue

            output_path = self.settings.out.joinpath(
                service.name + self.settings.final_name_suffix
            )
            tracing_filepath = service.joinpath("traces", "traces-merged-file.parquet")
            metrics_filepaths = list(service.joinpath("monitoring").glob("*.parquet"))

            joiner.start(tracing_filepath, metrics_filepaths, output_path)

    @timer
    def __concat_files(self, output_path: Path, additional_name: str):
        file_concat = FileConcat(self.settings)
        file_concat.start(output_path, additional_name)

    @timer
    def __calculate_statistics(self, path_list: dict) -> dict:
        found_folders = {}
        for name, folder in path_list.items():
            mon_size = sum([x.stat().st_size for x in folder["monitoring"]]) / 1_000_000
            trace_size = sum([x.stat().st_size for x in folder["traces"]]) / 1_000_000
            found_folders[
                name
            ] = f"{mon_size}MB of metric data, {trace_size}MB of trace data"

        return found_folders
