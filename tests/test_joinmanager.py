from controlflow.JoinManager import JoinManager


def test_start():
    join_manager = JoinManager()
    join_manager.settings.rounding_acc = "30m"
    join_manager.settings.test_mode = True
    join_manager.settings.out = "../out"
    join_manager.settings.source = (
        "/home/michaelleitner/Documents/contest/Data_TrainTicket/"
    )
    join_manager.settings.tree_settings.print_data_with_accessing_field = True
    join_manager.settings.tree_settings.accessing_field = 1

    join_manager.settings.visualize_graph = False
    join_manager.settings.neo4j_container_name = 'neo4j'
    join_manager.settings.neo4j_uri = "neo4j://localhost:7687"

    join_manager.process()


test_start()
