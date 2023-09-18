import subprocess
from pathlib import Path

from neo4j import GraphDatabase
from neo4jvis import StyledGraph

from jaeger_prometheus_joining.controlflow.ParseSettings import ParseSettings


class GraphGenerator:
    def __init__(self, settings: ParseSettings):
        self.settings = settings

    def start(self, source_path: Path, output_path: Path):
        self.__drop_all()
        self.__load_database(source_path)
        self.__generate_graph(output_path)

    def __drop_all(self):
        with GraphDatabase.driver(self.settings.neo4j_uri, auth=None) as driver:
            try:
                driver.execute_query("MATCH p=()-->() DELETE p")
                driver.execute_query("MATCH(n) DELETE n")
            except Exception as e:
                print(e)

    def __load_database(self, source_path: Path):
        subprocess.call(
            f"docker cp '{source_path.absolute()}' {self.settings.neo4j_container_name}:/var/lib/neo4j/import",
            shell=True,
        )

        with GraphDatabase.driver(self.settings.neo4j_uri, auth=None) as driver:
            try:
                driver.execute_query(
                    f"LOAD CSV WITH HEADERS FROM 'file:///{source_path.name}' AS line CREATE (s:Span) SET s += line"
                )
                driver.execute_query(
                    "MATCH (a:Span), (b:Span) WHERE a.childSpanId = b.spanID AND a.childTraceId = b.traceID CREATE (a)-[r:childOf]->(b)"
                )

            except Exception as e:
                print(e)

    def __generate_graph(self, output_path: Path):
        with GraphDatabase.driver(self.settings.neo4j_uri, auth=None) as driver:
            graph = StyledGraph(driver, directed=True)

            graph.options["layout"] = {
                "improvedLayout": "true",
                "hierarchical": {
                    "enabled": "true",
                    "treeSpacing": 400,
                    "nodeSpacing": 200,
                },
            }
            graph.options["physics"] = {
                "enabled": "false",
                "stabilization": {
                    "enabled": "false",
                    "fit": "false",
                },
            }

            QUERY = "MATCH p=()-[]->() RETURN p"
            graph.add_from_query(QUERY)

            # TO ADD NICER LABELS, CHANGE LINE 33 in query_utils.py of the library
            # for example:
            #         node1 = styled_graph.nodes[node1_id] \
            #             if node1_id in styled_graph.nodes \
            #             else StyledNode(node.id, node['container'])
            graph.draw(output_path)
