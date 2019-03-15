import copy
import uuid
import pytest
from blackfynn.models.workspace import Workspace


@pytest.fixture(scope="module")
def random_name():
    return "test-" + str(uuid.uuid4())


@pytest.fixture(scope="module")
def test_workspace_model(random_name):
    return Workspace(random_name, "my description")


@pytest.fixture(scope="module")
def test_workspace(client, test_workspace_model):
    workspace = client._api.workspaces.create(test_workspace_model)
    yield workspace

    client._api.workspaces.delete(workspace)


def test_create_workspace(client, test_workspace, test_workspace_model):
    assert test_workspace_model.name == test_workspace.name
    assert test_workspace_model.description == test_workspace.description


def test_get_workspace_by_name(client, test_workspace):
    retrieved_workspace = client.get_workspace(test_workspace.name)
    assert test_workspace == retrieved_workspace


def test_get_workspace_by_id(client, test_workspace):
    retrieved_workspace = client.get_workspace(test_workspace.id)
    assert test_workspace == retrieved_workspace


def test_get_all_workspaces(client, test_workspace):
    all_workspaces = client.workspaces()
    assert len(all_workspaces) > 0
    assert test_workspace in all_workspaces


@pytest.mark.xfail("fails because workspaces cannot be deleted")
def test_delete_workspace(client):
    workspace_name = "test-" + str(uuid.uuid4())
    workspace = client.create_workspace(workspace_name)
    workspace.delete()

    assert len(client.workspaces()) == 1  # workspace test fixutre still exists


def test_update_workspace(client, test_workspace):
    local_workspace = copy.copy(test_workspace)
    local_workspace.name = "Updated"
    updated_workspace = local_workspace.update()
    assert updated_workspace.name != test_workspace.name
    assert updated_workspace.name == "Updated"
