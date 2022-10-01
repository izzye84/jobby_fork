import networkx
from jobby.types.dag import DAG
from jobby.types.model import Model



def test_collapse_branching():
    graph = networkx.DiGraph()
    graph.add_edges_from([("0", "1"), ("1", "2"), ("2", "3")])
    networkx.set_node_attributes(graph, False, name="foundation")

    dag = DAG(graph=graph)

    models = {
        node: Model(
            name=node,
            unique_id=node,
            depends_on={edge[0] for edge in graph.in_edges(node)},
        )
        for node in dag.graph.nodes
    }

    new_selector = dag.generate_selector(models=models)

    assert new_selector == "0+,+3"


def test_collapse_multiple_branches():
    graph = networkx.DiGraph()
    graph.add_edges_from([("0", "1"), ("a", "2"), ("1", "2"), ("2", "3")])
    networkx.set_node_attributes(graph, False, name="foundation")

    dag = DAG(graph=graph)

    models = {
        node: Model(
            name=node,
            unique_id=node,
            depends_on={edge[0] for edge in graph.in_edges(node)},
        )
        for node in dag.graph.nodes
    }

    new_selector = dag.generate_selector(models=models)

    assert set(new_selector.split(" ")) == set("0+,+3 a+,+3".split(" "))


def test_foundation_transativity():
    graph = networkx.DiGraph()
    graph.add_edges_from([("0", "1"), ("a", "2"), ("1", "2"), ("2", "3"), ("2", "4")])
    networkx.set_node_attributes(graph, False, name="foundation")
    graph.nodes["0"]["foundation"] = True
    graph.nodes["a"]["foundation"] = True

    dag = DAG(graph=graph)

    models = {
        node: Model(
            name=node,
            unique_id=node,
            depends_on={edge[0] for edge in graph.in_edges(node)},
        )
        for node in dag.graph.nodes
    }

    new_selector = dag.generate_selector(models=models)

    assert set(new_selector.split(" ")) == set("+3 +4".split(" "))


def test_select_stability():
    graph = networkx.DiGraph()
    graph.add_edges_from([("0", "1"), ("a", "2"), ("1", "2"), ("2", "3"), ("2", "4")])
    networkx.set_node_attributes(graph, False, name="foundation")
    graph.nodes["0"]["foundation"] = True
    graph.nodes["a"]["foundation"] = True

    dag = DAG(graph=graph)

    # Select a subset of the graph!
    models_names = dag.select("1 a 2 3")

    models = {
        node: Model(
            name=node,
            unique_id=node,
            depends_on={edge[0] for edge in graph.in_edges(node)},
        )
        for node in dag.graph.nodes
        if node in models_names
    }

    new_selector = dag.generate_selector(models=models)

    assert set(new_selector.split(" ")) == set("1+,+3 a".split(" "))
