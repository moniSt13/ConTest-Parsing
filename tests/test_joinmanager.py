import time
from collections import Counter
from functools import reduce
from itertools import combinations

import jaeger_prometheus_joining.util.combinatorics
from controlflow.JoinManager import JoinManager
import polars as pl


def test():
    with open(
        "/home/michaelleitner/Documents/contest/Data_TrainTicket/ts-admin-basic-info-service-sprintstarterweb_1.5.22/LOGS_ts-admin-basic-info-service_springstarterweb_1.5.22.RELEASE.txt"
    ) as logfile:
        result_dict = {"timestamp": [], "origin": [], "log_status": [],"error_message": []}

        for line in logfile:
            if line.startswith("\t") or line.isspace() or not line[0].isdigit() or line.find("---") == -1:
                pass
            else:
                line_split_message_and_info = line.split(" : ")

                error_message = line_split_message_and_info[-1].strip()
                origin = line_split_message_and_info[0].split("] ")[1].strip()
                timestamp = "T".join(line_split_message_and_info[0].split("---")[0].split(" ")[0:2])
                log_status = line_split_message_and_info[0].split("---")[0].strip().split(" ")[-2].strip()

                # print(f"{line_cnt}: {timestamp} | {origin} | {error_message}")
                print(timestamp)
                result_dict["timestamp"].append(timestamp)
                result_dict["origin"].append(origin)
                result_dict["error_message"].append(error_message)
                result_dict["log_status"].append(log_status)

    # df = pl.DataFrame(result_dict)
    # df.write_csv("testout.csv")

test()

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


if __name__ == "__main__":
    test_start()
    print()
