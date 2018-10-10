import os
from datetime import datetime
from uuid import uuid4
from collections import namedtuple

import pytest

from blackfynn import Blackfynn
from blackfynn.models import ModelProperty, ModelPropertyType

from tests.utils import create_test_dataset, current_ts, get_test_client


@pytest.fixture(scope='session')
def client():
    """
    Login via API, return client. Login information, by default, will be taken from
    environment variables, so ensure those are set properly before testing. Alternatively,
    to force a particular user, adjust input arguments as necessary.
    """
    return get_test_client()


@pytest.fixture(scope='session')
def client2():
    api_token = os.environ.get('BLACKFYNN_API_TOKEN2')
    api_secret = os.environ.get('BLACKFYNN_API_SECRET2')
    assert api_token != "", "Must define BLACKFYNN_API_TOKEN2"
    assert api_secret != "", "Must define BLACKFYNN_API_SECRET2"

    bf = get_test_client(api_token=api_token, api_secret=api_secret)
    return bf


@pytest.fixture(scope='session')
def session_id():
    return "{}-{}".format(str(datetime.now()), str(uuid4())[:4])


@pytest.fixture(scope='session')
def dataset(client):
    """
    Test Dataset to be used by other tests.
    """
    ds = create_test_dataset(client)
    ds_id = ds.id
    all_dataset_ids = [x.id for x in client.datasets()]
    assert ds_id in all_dataset_ids

    # surface test dataset to other functions. Everything after the yield
    # serves as teardown code for the fixture
    yield ds

    # remove
    client._api.datasets.delete(ds)

    all_dataset_ids = [x.id for x in client.datasets()]
    assert ds_id not in all_dataset_ids
    assert not ds.exists
    assert not hasattr(ds, 'parent')


@pytest.fixture(scope='session')
def test_organization(client):
    return [o for o in client.organizations() if o.name == 'Blackfynn'][0]


Graph = namedtuple('TestGraph', ['dataset', 'models', 'model_records',
                                 'relationships', 'relationship_records'])


@pytest.fixture(scope="module")
def simple_graph(client):
    """
        Creates a small test graph in an independent dataset to de-couple
        from other tests
    """
    test_dataset = create_test_dataset(client)
    model_1 = test_dataset.create_model(
        'Model_A', description="model a", schema=[
            ModelProperty("prop1", data_type=ModelPropertyType(data_type=str),
                          title=True)])

    model_2 = test_dataset.create_model(
        'Model_B', description="model b",
        schema=[ModelProperty("prop1",
                              data_type=ModelPropertyType(data_type=str),
                              title=True)
                ])

    relationship = test_dataset.create_relationship_type(
        'New_Relationship_{}'.format(current_ts()), 'a new relationship')

    model_instance_1 = model_1.create_record({"prop1": "val1"})
    model_instance_2 = model_2.create_record({"prop1": "val1"})
    model_instance_1.relate_to(model_instance_2, relationship)

    graph = Graph(test_dataset, models=[model_1, model_2],
                  model_records=[model_instance_1, model_instance_2],
                  relationships=[relationship],
                  relationship_records=None)
    yield graph

    ds_id = test_dataset.id
    client._api.datasets.delete(test_dataset)
    all_dataset_ids = [x.id for x in client.datasets()]
    assert ds_id not in all_dataset_ids
    assert not test_dataset.exists
