import uuid
import pytest


@pytest.fixture(scope='module')
def graph_view(simple_graph):
    dataset = simple_graph.dataset
    name = 'patient-view-{}'.format(uuid.uuid4())

    view = dataset.create_view(name, 'patient', ['medication'])

    assert view.name == name
    assert view.root_model == 'patient'
    assert view.included_models == ['medication']
#    assert view.instance is not None

    return view


def test_refresh(graph_view):
    fresh = graph_view.refresh()

    assert fresh != graph_view
    assert fresh.id == graph_view.id
    assert fresh.instance != graph_view.instance
    assert fresh.name == graph_view.name
    assert fresh.root_model == graph_view.root_model
    assert fresh.include_models == graph_view.included_models


def test_get_view(simple_graph, graph_view):
    dataset = simple_graph.dataset

    # TODO: update this to retrieve by name
    got_view = dataset.get_view(graph_view.id)
    assert got_view.name == graph_view.name
    assert got_view.root_model == 'patient'
    assert got_view.included_models == ['medication']


def test_all_views(simple_graph, graph_view):
    dataset = simple_graph.dataset
    assert dataset.views() == [graph_view]


def test_graph_view_instances(simple_graph, graph_view):
    snapshot1 = graph_view.snapshot()
    snapshot2 = graph_view.snapshot()

    assert graph_view.all_snapshots() == [snapshot1, snapshot2]
    assert graph_view.latest() == snapshot2
