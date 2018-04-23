import os
import pytest
import uuid

from blackfynn import Blackfynn

SUPERADMIN_SECRET = os.environ['SUPERADMIN_SECRET']
SUPERADMIN_TOKEN = os.environ['SUPERADMIN_TOKEN']

TESTUSER_SECRET = os.environ['TESTUSER_SECRET']
TESTUSER_TOKEN = os.environ['TESTUSER_TOKEN']

# remoe these conflicting environment variables which will override the above
del os.environ['BLACKFYNN_API_TOKEN']
del os.environ['BLACKFYNN_API_SECRET']
os.environ['AWS_ACCESS_KEY_ID'] = "xxxxxxxxxxxxxxxx"
os.environ['AWS_SECRET_ACCESS_KEY'] = "xxxxxxxxxxxxxxxx"

def pytest_addoption(parser):
    parser.addoption("--devserver", default=[], help=("Test against dev server (not local)"))

def pytest_generate_tests(metafunc):
    print metafunc.fixturenames
    use_dev = False
    if 'devserver' in metafunc.fixturenames:
        use_dev = metafunc.config.option.devserver
    metafunc.parametrize("use_dev", [use_dev], scope='session')


@pytest.fixture(scope='session')
def client(use_dev):
    """
    Login via API, return client. Login information, by default, will be taken from
    environment variables, so ensure those are set properly before testing. Alternatively,
    to force a particular user, adjust input arguments as necessary.
    """
    assert TESTUSER_TOKEN is not None, "Please configure TESTUSER_TOKEN before testing"
    assert TESTUSER_SECRET is not None, "Please configure TESTUSER_SECRET before testing"

    bf = Blackfynn(
        api_token=TESTUSER_TOKEN,
        api_secret=TESTUSER_SECRET
    )
    # get organizations
    orgs = bf.organizations()
    print 'organizations =', orgs
    assert len(orgs) > 0

    # explicitly set context to Blackfyn org
    assert bf.context is not None
    return bf

@pytest.fixture(scope='session')
def client2(use_dev):
    bf = Blackfynn(
        api_token=TESTUSER_TOKEN,
        api_secret=TESTUSER_SECRET
    )
    # get organizations
    orgs = bf.organizations()
    assert len(orgs) > 0

    # explicitly set context to Blackfyn org
    assert bf.context is not None
    return bf


@pytest.fixture(scope='session')
def superuser_client(use_dev):
    """
    Client using super-admin permissions
    """

    assert SUPERADMIN_TOKEN is not None, "Please configure SUPERADMIN_TOKEN before testing"
    assert SUPERADMIN_SECRET is not None, "Please configure SUPERADMIN_SECRET before testing"

    bf = Blackfynn(
        api_token=SUPERADMIN_TOKEN,
        api_secret=SUPERADMIN_SECRET
    )
    assert bf.profile.is_super_admin
    return bf


@pytest.fixture(scope='session')
def dataset(use_dev, client, superuser_client):
    """
    Test Dataset to be used by other tests.
    """

    # collection of all datasets
    n_ds = len(client.datasets())

    # create test dataset
    ds = client.create_dataset("test dataset {}".format(uuid.uuid4()))
    assert ds.exists
    assert len(client.datasets()) == n_ds + 1

    # surface test dataset to other functions
    yield ds

    # remove
    superuser_client._api.datasets.delete(ds)

    assert len(client.datasets()) == n_ds
    assert not ds.exists
    assert not hasattr(ds, 'parent')

@pytest.fixture(scope='session')
def test_organization(client):
    return filter(lambda o: o.name == 'Test Organization', client.organizations())[0]

def test_login(client):
    email = os.environ.get('BLACKFYNN_USER')
    profile = client.profile
    print "profile = ", profile
    assert profile['email'] == email

