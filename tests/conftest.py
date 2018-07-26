import os
import pytest
from uuid import uuid4
from datetime import datetime
from blackfynn import Blackfynn

@pytest.fixture(scope='session')
def client():
    """
    Login via API, return client. Login information, by default, will be taken from
    environment variables, so ensure those are set properly before testing. Alternatively,
    to force a particular user, adjust input arguments as necessary.
    """
    bf = Blackfynn()
    # get organizations
    orgs = bf.organizations()
    print 'organizations =', orgs
    assert len(orgs) > 0

    # explicitly set context to Blackfyn org
    assert bf.context is not None
    return bf


@pytest.fixture(scope='session')
def client2():
    api_token = os.environ.get('BLACKFYNN_API_TOKEN2')
    api_secret = os.environ.get('BLACKFYNN_API_SECRET2')
    assert api_token != "", "Must define BLACKFYNN_API_TOKEN2"
    assert api_secret != "", "Must define BLACKFYNN_API_SECRET2"
    bf = Blackfynn()
    # get organizations
    orgs = bf.organizations()
    assert len(orgs) > 0

    # explicitly set context to Blackfyn org
    assert bf.context is not None
    return bf


@pytest.fixture(scope='session')
def session_id():
    return "{}-{}".format(str(datetime.now()), str(uuid4())[:4])

@pytest.fixture(scope='session')
def dataset(client, session_id):
    """
    Test Dataset to be used by other tests.
    """

    # collection of all datasets
    n_ds = len(client.datasets())

    # create test dataset
    ds = client.create_dataset("test dataset {}".format(session_id))
    ds_id = ds.id
    assert ds.exists
    all_dataset_ids = [x.id for x in client.datasets()]
    assert ds_id in all_dataset_ids

    # surface test dataset to other functions
    yield ds

    # remove
    client._api.datasets.delete(ds)

    all_dataset_ids = [x.id for x in client.datasets()]
    assert ds_id not in all_dataset_ids
    assert not ds.exists
    assert not hasattr(ds, 'parent')


@pytest.fixture(scope='session')
def test_organization(client):
    return filter(lambda o: o.name == 'Blackfynn', client.organizations())[0]
