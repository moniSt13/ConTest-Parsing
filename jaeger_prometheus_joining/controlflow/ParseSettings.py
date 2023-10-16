from pathlib import Path

from contest_tree.model.TreeSettings import TreeSettings


class ParseSettings:
    def __init__(self):
        self._source: Path = Path("./data")
        """Source path to the root of your data."""
        self._out: Path = Path("./out")
        """Output path of your data. The program saves states inbetween transformation steps. This folder should be 
        empty, because it will be cleaned!"""
        self.test_mode: bool = False
        """The program scans the whole input-folder and tries to parse everything that looks valid. If this member is 
        set to true it will only try to parse the first two found folders. If you have large services you can limit 
        the program even further. To do this look at `transformationscripts.FilepathFinder`"""
        self.rounding_acc: str = "30s"
        """Not every trace and metric has the same timestamp. To increase the likelyhood of finding joins, 
        we round the data. You can specify how exact you want to be."""
        self.save_to_disk: bool = True
        """If you set this member to false, nothing will be written to the disk. Slightly shorter runtime, 
        but no results. You can use this to determine if your data is parseable."""
        self.output_vis: bool = False
        """It is highly recommended to put any informational outputs after an `if` with this member. You can 
        determine errors by printing dataframes. Be careful because a lot of data ouput can slow down the program."""
        self.drop_null: bool = True
        """Metrics are sometimes recorded on pod and container level resulting in duplicated data. Your data size can 
        skyrocket if you disable this, but you will have more information. To restrict or change the behaviour look 
        at `transformationscripts.MetricsParser.__filter_data.`"""
        self.additional_name_tracing: str = "traces-"
        """Additional prefix for tracing outputs."""
        self.additional_name_metrics: str = "metrics-"
        """Additional prefix or metric ouputs."""
        self.final_name_suffix: str = "final"
        """Additional suffix for the final joined data."""
        self.clear_output: bool = True
        """If this member is set to true, the output folder will be cleared completely. It basically executes `rm -rf 
        {output}`. Be careful!"""
        self.print_statistics: bool = True
        """After starting the program it will print settings and folder sizes and other statistics. You can disable 
        this behaviour"""
        self.visualize_graph: bool = True
        """The default behaviour is to generate a html-based graph. This enables to get a grasp of your traces. Look 
        at `util.visualization` for more information."""
        self.neo4j_uri: str = "neo4j://localhost:7687"
        """Needed for visualizing the data. This is the database URI of your neo4j instance. Look at 
        `util.visualization` for more information."""
        self.neo4j_container_name: str = "neo4j"
        """If your neo4j instance is running in a docker environment, you can provide your containername here. Look 
        at `util.visualization` for more information."""
        self.tree_settings: TreeSettings = TreeSettings()
        """In the process of extracting even more information every trace will be parsed into a tree structure. You 
        can provide even more settings. The code for this functionality lies in another package (by the same author)"""

    @property
    def source(self) -> Path:
        """Source path to the root of your data."""
        return self._source

    @source.setter
    def source(self, new_source: str):
        self._source = Path(new_source)

    @property
    def out(self) -> Path:
        """Output path of your data. The program saves states inbetween transformation steps. This folder should be
        empty, because it will be cleaned!"""
        return self._out

    @out.setter
    def out(self, new_out: str):
        self._out = Path(new_out)

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
