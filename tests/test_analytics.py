import os
import io
import uuid
import time
import pytest
import tempfile


@pytest.fixture(scope="module")
def workspace(client):
    test_name = "test-" + str(uuid.uuid4())
    workspace = client.create_workspace(test_name)
    yield workspace

    client._api.workspaces.delete(workspace)


# This must be a module level fixture because views with duplicate
# root/included models are not allowed.
@pytest.fixture(scope='module')
def graph_view(workspace, simple_graph):
    dataset = simple_graph.dataset
    name = 'patient-view-{}'.format(uuid.uuid4())
    view = workspace.create_view(dataset, name, 'patient', ['medication'],
                                 create_snapshot=False)

    # add dataset object to view
    view.dataset = dataset

    assert view.name == name
    assert view.root_model == 'patient'
    assert view.included_models == ['medication']

    yield view

    view.delete()


@pytest.fixture(scope="module")
def query(workspace):
    query_str = "select * FROM some_table"
    query_name = "myquery"

    query = workspace.create_named_query(query_name, query_str)
    assert query.name == query_name
    assert query.query == query_str

    yield query

    query.delete()


@pytest.fixture(scope='module')
def ready_snapshot(graph_view):
    # wait for snapshot to process
    snapshot = graph_view.create_snapshot()
    start = time.time()
    while graph_view.latest() is None and (time.time() - start < 30):
        time.sleep(1)
    return graph_view.latest()


@pytest.fixture(scope='module')
def ready_query_results(client, workspace, ready_snapshot):
    table_name = ready_snapshot.view.name
    query_results = workspace.execute_query(
        'SELECT * FROM "{}"'.format(table_name))
    start = time.time()
    while query_results.status is not 'succeeded' and (time.time() - start < 30):
        time.sleep(1)
    return query_results


@pytest.fixture(scope='module')
def ready_query_execution_id(ready_query_results):
    return ready_query_results.execution_id


def test_get_view_definition(graph_view, workspace):
    assert workspace.get_view_definition(graph_view.id) == graph_view
    assert workspace.get_view_definition(graph_view.name) == graph_view


def test_all_views(graph_view, workspace):
    assert workspace.views() == [graph_view]


def test_view_versions(graph_view, workspace):
    n_snaps1 = len(graph_view.snapshots(status='any'))
    v2 = graph_view.create_snapshot()
    v3 = graph_view.create_snapshot()
    assert v2.status == 'processing'
    assert v3.status == 'processing'

    n_snaps2 = len(graph_view.snapshots(status='any'))
    assert n_snaps2-n_snaps1 == 2

    # Can't compare lists of versions  because other instances
    # are created by other tests
    versions = graph_view.snapshots(status='any')
    assert versions[-2] == v2
    assert versions[-1] == v3
    assert graph_view.latest(status='any') == v3

    # no failed snapshots should exist
    assert graph_view.snapshots(status='failed') == []
    assert graph_view.latest(status='failed') is None


def test_get_snapshot(graph_view):
    v1 = graph_view.create_snapshot()
    v2 = graph_view.get_snapshot(v1.id)
    assert v1 == v2
    assert v1.created_at == v2.created_at


def test_latest_with_no_snapshots_create_one(workspace, simple_graph):
    dataset = simple_graph.dataset

    # Create a view with no snapshots
    view = dataset._api.analytics.create_view(
        workspace, dataset, 'patient-view-{}'.format(uuid.uuid4()), 'patient',
        [])

    assert view.snapshots(status='any') == []
    assert view.latest(status='any') is None

    view.create_snapshot()

    snapshots = view.snapshots(status='any')
    assert len(snapshots) == 1
    assert view.latest(status='any') == snapshots[0]


def test_delete_view(workspace, simple_graph):
    dataset = simple_graph.dataset
    view = workspace.create_view(dataset, 'medication-view', 'medication', [])
    assert view.exists
    view.delete()
    assert 'medication-view' not in [v.name for v in workspace.views()]
    assert not view.exists


def test_cant_create_duplicate_views(workspace, simple_graph, graph_view):
    dataset = simple_graph.dataset
    with pytest.raises(Exception):
        workspace.create_view(dataset, graph_view.name,
                              'medication', ['patient'])

    with pytest.raises(Exception):
        workspace.create_view(dataset,
                              'different-name-same-models',
                              graph_view.root_model,
                              graph_view.included_models)


def test_as_dataframe_not_ready(graph_view):
    with pytest.raises(Exception):
        graph_view.latest(status='processing').as_dataframe()


def test_snapshot_as_dataframe(ready_snapshot):
    df = ready_snapshot.as_dataframe()
    assert set(df.columns) == set(
        ['patient', 'patient.name', 'medication', 'medication.name'])


def test_snapshot_as_json(ready_snapshot):
    json_resp = ready_snapshot.as_json()
    for obj in json_resp:
        assert 'patient.name' in obj
        assert 'medication.name' in obj


@pytest.mark.parametrize('format', ['parquet', 'json'])
def test_download_file(ready_snapshot, format):
    path = os.path.join(
        tempfile.gettempdir(),
        '{snap}.{format}'.format(snap=ready_snapshot.id, format=format))
    assert not os.path.exists(path)
    ready_snapshot.download(path, format=format)
    assert os.path.exists(path)
    os.remove(path)
    assert not os.path.exists(path)


@pytest.mark.parametrize('format', ['parquet', 'json'])
def test_download_buffer(ready_snapshot, format):
    buff = io.BytesIO()
    assert len(buff.read()) == 0
    ready_snapshot.download(buff, format=format)
    buff.seek(0)
    assert len(buff.read(10)) == 10
    buff.close()
    del buff


def _block_until_ready(view, snapshot):
    start = time.time()
    while snapshot.status != 'Ready' and (time.time() - start < 30):
        time.sleep(1)
        snapshot = view.get_snapshot(snapshot.id)
    return snapshot


def test_delete_snapshot(graph_view):
    v1 = graph_view.create_snapshot()
    v2 = _block_until_ready(graph_view, v1)

    status = v2.delete()
    v3 = graph_view.get_snapshot(v2.id)
    assert v3 is None


def test_delete_snapshot_from_view(graph_view):
    v1 = graph_view.create_snapshot()
    v2 = _block_until_ready(graph_view, v1)

    graph_view.delete_snapshot(v2.id)
    v3 = graph_view.get_snapshot(v2.id)
    assert v3 is None


@pytest.mark.xfail
def test_delete_contents(client, simple_graph):
    new_random_name = "test-" + str(uuid.uuid4())
    workspace = client.create_workspace(new_random_name)
    dataset = simple_graph.dataset
    name = 'patient-only-{}'.format(uuid.uuid4())
    view = workspace.create_view(dataset, name, 'patient', [],
                                 create_snapshot=False)
    assert len(workspace.views()) == 1
    workspace.delete_contents()
    assert len(workspace.views()) == 0

    # Cleanup
    client._api.workspaces.delete(workspace)


def test_get_named_query(workspace, query):
    assert workspace.get_named_query(query.id) == query
    assert workspace.get_named_query(query.name) == query


def test_get_all_named_queries(workspace, query):
    assert workspace.queries() == [query]


def test_delete_named_query(workspace):
    query2 = workspace.create_named_query(
        "query2", "select * FROM some_table")
    assert query2 in workspace.queries()
    query2.delete()
    assert query2 not in workspace.queries()


@pytest.mark.xfail
def test_delete_all_named_queries(client):
    test_name = "test-" + str(uuid.uuid4())
    workspace = client.create_workspace(test_name)

    for i in range(3):
        workspace.create_named_query(
            "query{}".format(str(i)), "SELECT * FROM some_table")

    assert len(workspace.queries()) == 3
    workspace.delete_all_named_queries()
    assert len(workspace.queries()) == 0
    workspace.delete()


def test_update_named_query(workspace):
    query = workspace.create_named_query(
        "query2", "SELECT * FROM some_table")
    query.name = "Updated"
    updated_query = query.update()
    assert updated_query.name == "Updated"
    assert updated_query.query == query.query
    updated_query.delete()


def test_execute_query(workspace, ready_snapshot):
    table_name = ready_snapshot.view.name
    results = workspace.execute_query(
        'SELECT * FROM "{}"'.format(table_name))
    assert results.workspace_id == workspace.id


def test_execute_named_query(workspace, ready_snapshot):
    table_name = ready_snapshot.view.name
    query = workspace.create_named_query(
        "Live", 'SELECT * FROM "{}"'.format(table_name))
    results = query.execute()
    assert results.workspace_id == workspace.id


def test_query_results_status(ready_query_results):
    assert ready_query_results.status == "succeeded"


def test_download_query_results(ready_query_results):
    path = os.path.join(
        tempfile.gettempdir(),
        '{}.csv'.format(ready_query_results.execution_id))
    assert not os.path.exists(path)
    ready_query_results.download(path)
    assert os.path.exists(path)
    os.remove(path)
    assert not os.path.exists(path)


def test_query_results_as_dataframe(ready_query_results):
    query_df = ready_query_results.as_dataframe()
    assert len(query_df) > 0
