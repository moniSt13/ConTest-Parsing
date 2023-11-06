import json

import polars as pl
from polars import Struct, Field, Utf8, List, Float64, last, col, Float32

from controlflow.JoinManager import JoinManager


def test_start():
    join_manager = JoinManager()
    join_manager.settings.rounding_acc = "1ms"
    join_manager.settings.drop_null = False
    join_manager.settings.test_mode = False
    join_manager.settings.out = "../out"
    join_manager.settings.source = (
        "/home/michaelleitner/Documents/contest/Data_TrainTicket/"
    )
    # join_manager.settings.source = (
    #     "/home/michaelleitner/Documents/contest/Data_TrainTicket_Pirmin/"
    # )
    join_manager.settings.output_vis = False
    join_manager.settings.tree_settings.print_data_with_accessing_field = True
    join_manager.settings.tree_settings.accessing_field = 1

    join_manager.settings.visualize_graph = False
    join_manager.settings.neo4j_container_name = "neo4j"
    join_manager.settings.neo4j_uri = "neo4j://localhost:7687"

    join_manager.process()


if __name__ == "__main__":
    test_start()
