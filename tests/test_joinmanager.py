import time
from collections import Counter
from functools import reduce
from itertools import combinations

import jaeger_prometheus_joining.util.combinatorics
from controlflow.JoinManager import JoinManager








test = [
    {"name": "ts-admin", "value": "bla1.0"},
    {"name": "ts-admin", "value": "bla1.1"},
    {"name": "ts-admin", "value": "bla1.2"},
    {"name": "ts-seat", "value": "bla2.0"},
    {"name": "ts-seat", "value": "bla2.1"},
    {"name": "ts-seat", "value": "bla2.2"},
    {"name": "ts-security", "value": "bla4.0"},
    {"name": "ts-security", "value": "bla4.1"},
    {"name": "ts-another", "value": "bla5"},
    {"name": "ts-another", "value": "bla5"},
    {"name": "ts-another", "value": "bla5"},
    {"name": "ts-another", "value": "bla5"},
    {"name": "ts-another", "value": "bla5"},
    {"name": "ts-another", "value": "bla5"},
    {"name": "ts-another", "value": "bla5"},
    {"name": "ts-another", "value": "bla5"},
    {"name": "ts-another", "value": "bla5"},
    {"name": "ts-another", "value": "bla5"},
    {"name": "ts-another", "value": "bla5"},
    {"name": "ts-another", "value": "bla5"},
    {"name": "ts-another", "value": "bla5"},
    {"name": "ts-another", "value": "bla5"},
    {"name": "ts-another", "value": "bla5"},
    {"name": "ts-another", "value": "bla5"},
    {"name": "ts-another", "value": "bla5"},
    {"name": "ts-another", "value": "bla5"},
    {"name": "ts-another", "value": "bla5"},
    {"name": "ts-another", "value": "bla5"},
    {"name": "ts-another", "value": "bla5"},
    {"name": "ts-another", "value": "bla5"},
    {"name": "ts-another2", "value": "bla5"},
    {"name": "ts-another2", "value": "bla5"},
    {"name": "ts-another2", "value": "bla5"},
    {"name": "ts-another2", "value": "bla5"},
    {"name": "ts-another2", "value": "bla5"},
    {"name": "ts-another2", "value": "bla5"},
    {"name": "ts-another2", "value": "bla5"},
    {"name": "ts-another2", "value": "bla5"},
    {"name": "ts-another3", "value": "bla5"},
    {"name": "ts-another3", "value": "bla5"},
    {"name": "ts-another3", "value": "bla5"},
    {"name": "ts-another3", "value": "bla5"},
    # {"name": "ts-another3", "value": "bla5"},
    # {"name": "ts-another3", "value": "bla5"},
    # {"name": "ts-another3", "value": "bla5"},
    # {"name": "ts-another3", "value": "bla5"},
    # {"name": "ts-another3", "value": "bla5"},
    {"name": "ts-another5", "value": "bla5"},
    {"name": "ts-another5", "value": "bla5"},
    {"name": "ts-another5", "value": "bla5"},
    {"name": "ts-another5", "value": "bla5"},
    {"name": "ts-another5", "value": "bla5"},
    # {"name": "ts-another5", "value": "bla5"},
    # {"name": "ts-another5", "value": "bla5"},
    # {"name": "ts-another6", "value": "bla5"},
    # {"name": "ts-another6", "value": "bla5"},
    # {"name": "ts-another6", "value": "bla5"},
    # {"name": "ts-another6", "value": "bla5"},
    # {"name": "ts-another6", "value": "bla5"},
    # {"name": "ts-another6", "value": "bla5"},
    # {"name": "ts-another6", "value": "bla5"},
    # {"name": "ts-another6", "value": "bla5"},
    # {"name": "ts-another6", "value": "bla5"},
]
# now = time.perf_counter()
# all_combinations = jaeger_prometheus_joining.util.combinatorics.get_all_combinations(test, "name")
# print(time.perf_counter() - now)
# print(len(all_combinations))
# for x in all_combinations:
#     print(x)


def test_start():
    join_manager = JoinManager()
    join_manager.settings.rounding_acc = "30m"
    join_manager.settings.test_mode = False
    join_manager.settings.out = "../out"
    join_manager.settings.source = (
        "/home/michaelleitner/Documents/contest/Data_TrainTicket/"
    )
    join_manager.settings.tree_settings.print_data_with_accessing_field = True
    join_manager.settings.tree_settings.accessing_field = 1

    join_manager.settings.visualize_graph = False
    join_manager.settings.neo4j_container_name = "neo4j"
    join_manager.settings.neo4j_uri = "neo4j://localhost:7687"

    join_manager.process()

if __name__ == '__main__':
    test_start()
