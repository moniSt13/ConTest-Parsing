"""
 It adds fields for the min/mean/max-depth of traces. To achieve this we parse every trace in a tree structure.
 This functionality provides the package `contest-tree` which is not in this repository.
"""

import os
from pathlib import Path

import polars as pl
from contest_tree.model.Node import Node
from contest_tree.model.Root import Root
from polars import col, Utf8

from jaeger_prometheus_joining.controlflow.ParseSettings import ParseSettings


class TreeBuilder:
    def __init__(self, settings: ParseSettings):
        self.settings: ParseSettings = settings

    def start(self, source_path: Path, output_path):
        # schema = {"traceID": Utf8, "spanID": Utf8, "childTraceID":Utf8, "childSpanID":Utf8}
        df = pl.read_csv(source_path, dtypes={"http.status_code": Utf8})

        trace_stats_df = self.__parse_and_build_tree(df)
        updated_df = self.__join_with_data(df, trace_stats_df)
        self.__write_to_disk(updated_df, output_path)

    def __parse_and_build_tree(self, df: pl.DataFrame) -> pl.DataFrame:
        def __build_tree(cur_node: Node):
            lookup = cur_node.data[1]
            found_spans = df.filter(col("childSpanID") == lookup)
            for span in found_spans.iter_rows():
                node = Node(data=span)
                cur_node.add_children([node])
                __build_tree(node)

        root_spans = df.filter(col("childSpanID") == None)
        final_df_list = []

        for cur_root in root_spans.iter_rows():
            root = Root(data=cur_root)

            __build_tree(root)
            tree = root.init_tree()
            tree.settings = self.settings.tree_settings

            final_df_list.append(pl.from_dicts(tree.to_polars_readable_format()))

        return pl.concat(final_df_list)

    def __join_with_data(
        self, df: pl.DataFrame, trace_stats_df: pl.DataFrame
    ) -> pl.DataFrame:
        return df.join(trace_stats_df, on="spanID")

    def __write_to_disk(self, df: pl.DataFrame, output_path: Path):
        if not os.path.exists(output_path.parents[0]):
            os.makedirs(output_path.parents[0])

        df.write_csv(output_path)
