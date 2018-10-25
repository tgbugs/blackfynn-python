import uuid
import pytest

from blackfynn import models


# This must be a module level fixture because views with duplicate
# root/included models are not allowed.
@pytest.fixture(scope='module')
def graph_view(simple_graph):
    dataset = simple_graph.dataset
    name = 'patient-view-{}'.format(uuid.uuid4())
    view = dataset.create_view(name, 'patient', ['medication'])

    assert view.name == name
    assert view.root_model == 'patient'
    assert view.included_models == ['medication']

    yield view

    view.delete()


def test_get_view(graph_view):
    dataset = graph_view.dataset
    assert dataset.get_view(graph_view.id) == graph_view.latest()
    assert dataset.get_view(graph_view.name) == graph_view.latest()


def test_all_views(graph_view):
    dataset = graph_view.dataset
    assert dataset.views() == [graph_view]


def test_view_versions(graph_view):
    v2 = graph_view.create_snapshot()
    v3 = graph_view.create_snapshot()

    # Can't compare lists of versions  because other instances
    # are created by other tests
    versions = graph_view.get_snapshots()
    assert versions[-2] == v2
    assert versions[-1] == v3

    assert graph_view.latest() == v3


def test_latest_with_no_instances_creates_one(simple_graph):
    dataset = simple_graph.dataset

    # Create a view with no instances
    view = dataset._api.analytics.create_view(
        dataset, 'patient-view-{}'.format(uuid.uuid4()), 'patient', [])

    assert view.get_snapshots() == []
    assert isinstance(view.latest(), models.GraphViewSnapshot)


def test_delete_view(simple_graph):
    dataset = simple_graph.dataset
    view = dataset.create_view('medication-view', 'medication', [])
    assert view.exists
    view.delete()
    assert 'medication-view' not in [v.name for v in dataset.views()]
    assert not view.exists


def test_cant_create_duplicate_views(graph_view):
    dataset = graph_view.dataset
    with pytest.raises(Exception):
        dataset.create_view(graph_view.name, 'medication', ['patient'])

    with pytest.raises(Exception):
        dataset.create_view('different-name-same-models', graph_view.root_model, graph_view.included_models)


def test_as_dataframe(graph_view):
    df = graph_view.latest().as_dataframe()
    assert set(df.columns) == set(['patient', 'patient.name', 'medication', 'medication.name'])


def test_as_json(graph_view):
    json = graph_view.latest().as_json()
    for obj in json:
        assert 'patient.name' in obj
        assert 'medication.name' in obj
