from controlflow.JoinManager import JoinManager


def test_start():
    join_manager = JoinManager()
    join_manager.settings.rounding_acc = "5m"
    join_manager.settings.test_mode = False
    join_manager.settings.out = "../out"
    join_manager.settings.source = (
        "/home/michaelleitner/Documents/contest/Data_TrainTicket/"
    )
    join_manager.settings.tree_settings.print_data_with_accessing_field = True
    join_manager.settings.tree_settings.accessing_field = 1

    join_manager.process()


test_start()
