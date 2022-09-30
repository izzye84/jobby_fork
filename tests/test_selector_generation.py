import networkx
from jobby.dag import DAG
from jobby.schemas import Model


def test_collapse_branching():
    graph = networkx.DiGraph()
    graph.add_edges_from([("0", "1"), ("1", "2"), ("2", "3")])

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

    assert new_selector == '0+,+3'


def test_collapse_multiple_branches():
    graph = networkx.DiGraph()
    graph.add_edges_from([("0", "1"), ("a", "2"), ("1", "2"), ("2", "3")])

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

    assert set(new_selector.split(" ")) == set('0+,+3 a+,+3'.split(" "))
