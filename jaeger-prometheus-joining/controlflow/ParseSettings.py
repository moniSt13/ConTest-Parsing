from pathlib import Path


class ParseSettings:
    def __init__(self):
        self.source: Path = Path("../data")
        self.out: Path = Path("../out")
        self.test_mode: bool = False
        self.rounding_acc: str = "30s"
        self.save_to_disk: bool = True
        self.output_vis: bool = False
        self.drop_null: bool = True
        self.additional_name_tracing: str = "traces-"
        self.additional_name_metrics: str = "metrics-"
        self.final_name_suffix: str = "-final.csv"

    def __str__(self):
        return (
            f"source: {self.source}\n"
            f"out: {self.out}\n"
            f"test_mode: {self.test_mode}\n"
            f"rounding_acc: {self.rounding_acc}\n"
            f"save_to_disk: {self.save_to_disk}\n"
            f"output_vis: {self.output_vis}\n"
            f"drop_null: {self.drop_null}\n"
            f"additional_name_tracing: {self.additional_name_tracing}\n"
            f"additional_name_metrics: {self.additional_name_metrics}\n"
        )
