from neo4jvis.model.styled_node import StyledNode
from neo4jvis.model.styled_edge import StyledEdge


def query_relationships(driver, limit=100):
    QUERY_RELATIONSHIP = f"""
        MATCH (n)
            WITH n, rand() AS random
            ORDER BY random
            LIMIT {limit}
            OPTIONAL MATCH (n)-[r]->(m)
            RETURN id(n) AS source_id,
                   labels(n) as source_label,
                   id(r) as relation_id,
                   type(r) as label,
                   id(m) AS target_id,
                   labels(m) as target_label
    """
    result = driver.session().run(QUERY_RELATIONSHIP)
    return result


def add_from_relationship(
        driver,
        styled_graph,
        query):
    result = driver.session().run(query)

    def __get_node(node, styled_graph):
        node1_id = node.id
        node1 = styled_graph.nodes[node1_id] \
            if node1_id in styled_graph.nodes \
            else StyledNode(node.id, node['spanID'])
        for k in node.keys():
            node1[k] = node[k]
        return node1



    for row in result:
        # NOTE: WE ARE ONLY GETTING THE FIRST RELATIONSHIP HERE
        relationship = list(row["p"].relationships)[0]
        node1 = __get_node(row["p"].start_node, styled_graph)
        node2 = __get_node(row["p"].end_node, styled_graph)
        styled_graph.nodes[node1.id] = node1
        styled_graph.nodes[node2.id] = node2
        edge = StyledEdge(relationship.id, node1, node2, relationship.type)
        for k in relationship.keys():
            edge[k] = relationship[k]
        node1.edges.add(edge)
        node2.edges.add(edge)
        styled_graph.edges.add(edge)

def generate_whole_graph(graph, limit=100):
    result = query_relationships(graph.driver, limit)
    nodes = {}
    edges = set()
    for row in result:
        if row['source_id'] is not None:
            id1 = str(row['source_id'])
            label1 = row['source_label']
            node1 = nodes[id1] if id1 in nodes else StyledNode(id1, label1)
            nodes[id1] = node1
        if row['target_id'] is not None:
            id2 = str(row['target_id'])
            label2 = row['target_label']
            node2 = nodes[id2] if id2 in nodes else StyledNode(id2, label2)
            nodes[id2] = node2
        if row['source_id'] is not None and row['target_id'] is not None:
            edge_id = row['relation_id']
            edge = StyledEdge(edge_id, node1, node2, param_type=str(row['label']))
            edges.add(edge)
            node1.edges.add(edge)
            node2.edges.add(edge)

    graph.edges = edges
    graph.nodes = nodes
    return graph
