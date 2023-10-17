"""
This is the class, which controls the 'parsing-flow'. You can change the order, in which the program transforms the data.
Take a look at the `process`-method!
"""

import shutil
from pathlib import Path

from jaeger_prometheus_joining.controlflow.ParseSettings import ParseSettings
from jaeger_prometheus_joining.featureengineering.TreeBuilder import TreeBuilder
from jaeger_prometheus_joining.transformationscripts.FileConcat import FileConcat
from jaeger_prometheus_joining.transformationscripts.FilepathFinder import (
    FilepathFinder,
)
from jaeger_prometheus_joining.transformationscripts.Joiner import Joiner
from jaeger_prometheus_joining.transformationscripts.MetricsParser import MetricsParser
from jaeger_prometheus_joining.transformationscripts.TraceInOneRowExploder import (
    TracesInOneRowExploder,
)
from jaeger_prometheus_joining.transformationscripts.TracesParser import TracesParser
from jaeger_prometheus_joining.util.timedecorator import timer
from jaeger_prometheus_joining.util.visualization.GraphGenerator import GraphGenerator


class JoinManager:
    def __init__(self):
        self.settings: ParseSettings = ParseSettings()
        """Seemingly the most important class member. You can define every setting, which effects the whole program. 
        Take a look at the `ParseSettings` documentation for further help."""

    def process(self):
        """
        This method defines the core logic. The default sequence looks like this:
        * scanning your input_folder for parseable files
        * printin statistics (folder size, found services, settings)
        * clears output folder
        * parsing metrics
        * parsing traces and concatenating them
        * joining metrics and traces
        * feature-engineer the new data to gain even more value (min/mean/max trace depth, etc.)
        * generate graphs of your data (html rendered graphs with neo4j and vis.js)
        * imploding the data trace-wise (to fit to a specific model)
        :return:
        """
        path_list = FilepathFinder(self.settings).find_files()

        self.__print_statistics(path_list)
        self.__clear_output()
        self.__parse_metrics(path_list)
        self.__parse_traces(path_list)
        self.__join()
        self.__feature_engineering()
        self.__generate_graph()
        self.__explode_trace_into_one_line()

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
            if service.is_file():
                continue

            output_path = self.settings.out.joinpath(
                service.name, f"{service.name}-{self.settings.final_name_suffix}.csv"
            )
            tracing_filepath = service.joinpath("traces", "traces-merged-file.parquet")
            metrics_filepaths = list(service.joinpath("monitoring").glob("*.parquet"))

            joiner.start(tracing_filepath, metrics_filepaths, output_path)

    @timer
    def __feature_engineering(self):
        tree_builder = TreeBuilder(self.settings)

        for service in self.settings.out.iterdir():
            if service.is_file():
                continue

            source_path = self.settings.out.joinpath(
                service.name, f"{service.name}-{self.settings.final_name_suffix}.csv"
            )

            output_path = source_path

            tree_builder.start(source_path, output_path)

    @timer
    def __generate_graph(self):
        if not self.settings.visualize_graph:
            return

        graph_generator = GraphGenerator(self.settings)

        for service in self.settings.out.iterdir():
            if service.is_file():
                continue

            source_path = self.settings.out.joinpath(
                service.name, f"{service.name}-{self.settings.final_name_suffix}.csv"
            )

            output_path = self.settings.out.joinpath(
                service.name, "visualized-graph.html"
            )
            graph_generator.start(source_path, output_path)

    @timer
    def __explode_trace_into_one_line(self):
        exploder = TracesInOneRowExploder(self.settings)
        for service in self.settings.out.iterdir():
            if service.is_file():
                continue

            source_path = self.settings.out.joinpath(
                service.name, f"{service.name}-{self.settings.final_name_suffix}.csv"
            )

            output_path = self.settings.out.joinpath(
                service.name,
                f"{service.name}-{self.settings.final_name_suffix}-exploded.csv",
            )

            exploder.start(source_path, output_path)

    def __concat_files(self, output_path: Path, additional_name: str):
        file_concat = FileConcat(self.settings)
        file_concat.start(output_path, additional_name)

    @timer
    def __clear_output(self):
        if not self.settings.clear_output:
            return

        if self.settings.out.exists() and self.settings.out.is_dir():
            shutil.rmtree(self.settings.out)

    @timer
    def __print_statistics(self, path_list: dict):
        if self.settings.print_statistics:
            found_folders = {}
            for name, folder in path_list.items():
                mon_size = (
                    sum([x.stat().st_size for x in folder["monitoring"]]) / 1_000_000
                )
                trace_size = (
                    sum([x.stat().st_size for x in folder["traces"]]) / 1_000_000
                )
                found_folders[
                    name
                ] = f"{mon_size}MB of metric data, {trace_size}MB of trace data"

            stats = "\n".join([f"{k}: {v}" for k, v in found_folders.items()])

            print(
                f"\n\n\n___ Following settings found: \n"
                f"{self.settings}\n\n"
                f"___ Found following folders to process: \n"
                f"{stats}\n\n"
                f"___ Processing time: \n"
            )
