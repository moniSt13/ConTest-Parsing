from controlflow.JoinManager import JoinManager


def test_start():
    joinmanager = JoinManager()
    joinmanager.settings.rounding_acc = "5s"
    joinmanager.settings.test_mode = True
    joinmanager.settings.out = "../out"
    joinmanager.settings.source = (
        "/home/michaelleitner/Documents/contest/Data_TrainTicket/"
    )

    joinmanager.process()


test_start()
