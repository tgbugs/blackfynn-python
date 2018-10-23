import uuid
import pytest

from blackfynn.models import Record


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
    assert view.instance is not None

    return view


def test_refresh(graph_view):
    fresh = graph_view.refresh()

    assert fresh != graph_view
    assert fresh.id == graph_view.id
    assert fresh.instance != graph_view.instance
    assert fresh.name == graph_view.name
    assert fresh.root_model == graph_view.root_model
    assert fresh.included_models == graph_view.included_models


def test_get_view(graph_view):
    dataset = graph_view.dataset
    assert dataset.get_view(graph_view.id) == graph_view.latest()
    assert dataset.get_view(graph_view.name) == graph_view.latest()


def test_all_views(graph_view):
    dataset = graph_view.dataset
    assert dataset.views() == [graph_view.latest()]


def test_view_versions(graph_view):
    v2 = graph_view.refresh()
    v3 = graph_view.refresh()

    # Can't compare lists of versions  because other instances
    # are created by other tests
    versions = graph_view.versions()
    assert versions[0] == graph_view
    assert versions[-2] == v2
    assert versions[-1] == v3

    assert graph_view.latest() == v3


def test_latest_with_no_instances_creates_one(simple_graph):
    dataset = simple_graph.dataset

    # Create a view with no instances
    view = dataset._api.analytics.create_view(
        dataset, 'patient-view-{}'.format(uuid.uuid4()), 'patient', [])
    assert view.instance is None

    view = view.latest()
    assert view.instance is not None


def test_cant_create_duplicate_views(graph_view):
    dataset = graph_view.dataset
    with pytest.raises(Exception):
        dataset.create_view(graph_view.name, 'medication', ['patient'])

    with pytest.raises(Exception):
        dataset.create_view('different-name-same-models', graph_view.root_model, graph_view.included_models)


def test_as_dataframe(graph_view, simple_graph):
    df = graph_view.as_dataframe()
    patient_model, medication_model = simple_graph.models

    assert set(df.columns) == set(['patient', 'patient.name', 'medication', 'medication.name'])

    assert all([isinstance(r, Record) for r in df['patient']])
    assert all([isinstance(r, Record) or r is None for r in df['medication']])


def test_as_json(graph_view):
    json = graph_view.as_json()
    for obj in json:
        assert 'patient.name' in obj
        assert 'medication.name' in obj
