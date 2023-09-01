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
        self.clear_output: bool = True
        self.print_statistics: bool = True

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
            f"final_name_suffix: {self.final_name_suffix}\n"
            f"clear_output: {self.clear_output}\n"
            f"print_statistics: {self.print_statistics}\n"
        )
