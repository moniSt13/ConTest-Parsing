import time
from collections import Counter
from functools import reduce
from itertools import combinations

import jaeger_prometheus_joining.util.combinatorics
from controlflow.JoinManager import JoinManager
import polars as pl

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
