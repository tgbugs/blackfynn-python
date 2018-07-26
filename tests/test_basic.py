import os
import pytest
import requests
import datetime

# client library
from blackfynn.models import (
    DataPackage, Dataset, File
)
from blackfynn.base import UnauthorizedException


def test_update_dataset(client, dataset, session_id):
    # update name of dataset
    ds_name = 'Same Dataset, Different Name {}'.format(session_id)
    dataset.name = ds_name
    dataset.update()
    ds2 = client.get_dataset(dataset.id)
    assert ds2.id == dataset.id
    assert ds2.name == ds_name

def test_datasets(dataset):
    ds_items = len(dataset)

    # create package locally
    pkg = DataPackage('Child of Dataset', package_type='Text')

    assert not pkg.exists
    assert pkg not in dataset

    # add package to dataset
    dataset.add(pkg)

    assert pkg.exists
    assert pkg in dataset
    assert pkg.id in dataset
    assert len(dataset) == ds_items + 1

    # remove from dataset
    dataset.remove(pkg)

    assert not pkg.exists
    assert pkg not in dataset
    assert len(dataset) == ds_items

# def test_dataset_collaborators(client, client2, test_organization, dataset):
#     # share with user
#     user2Id = client2.profile.id
#     user_share_resp = dataset.add_collaborators(user2Id)
#     assert user_share_resp['changes'][user2Id]['success']

#     # unshare with user
#     user_remove_resp = dataset.remove_collaborators(user2Id)
#     assert user_remove_resp['changes'][user2Id]['success']

#     # # share with org
#     org_share_resp = dataset.add_collaborators(test_organization.id)
#     assert org_share_resp['changes'][test_organization.id]['success']

#     # # unshare with org
#     org_remove_resp = dataset.remove_collaborators(test_organization.id)
#     assert org_remove_resp['changes'][test_organization.id]['success']

def test_packages_create_delete(client, dataset):

    # init
    pkg = DataPackage('Some MRI', package_type='MRI')
    assert not pkg.exists

    # create
    dataset.add(pkg)
    assert pkg.exists
    assert pkg.id is not None
    assert pkg.name == 'Some MRI'

    # TODO: (once we auto-include in parent)
    assert pkg in dataset

    # update package name
    pkg.name = 'Some Other MRI'
    pkg = client.update(pkg)

    pkg2 = client.get(pkg.id)

    assert pkg2.name == 'Some Other MRI'
    assert pkg2.id == pkg.id

    # delete all packages
    client.delete(pkg)

    assert not pkg.exists

    pkg = DataPackage('Something else', package_type='TimeSeries')
    assert not pkg.exists
    dataset.add(pkg)
    assert pkg.exists
    pid = pkg.id
    pkg.delete()
    assert not pkg.exists

    pkg2 = client.get(pid)
    assert pkg2 is None

    # TODO: (once we auto-remove from parent)
    #assert pkg not in dataset

def test_package_states(client, dataset):
    pkg = DataPackage('My Stateful Package', package_type='Slide')
    assert not pkg.exists
    dataset.add(pkg)
    assert pkg.exists
    assert pkg.state == "UNAVAILABLE"
    pkg.set_ready()
    pkg2 = client.get(pkg.id)
    assert pkg2.id == pkg.id
    assert pkg2.state == pkg.state
    assert pkg2.state == "READY"
    pkg.set_error()
    del pkg2

    pkg2 = client.get(pkg.id)
    assert pkg2.id == pkg.id
    assert pkg2.state == pkg.state
    assert pkg2.state == "ERROR"
    del pkg2

    pkg.delete()


def test_properties(client, dataset):

    pkg = DataPackage('Some Video', package_type='Video')
    assert not pkg.exists

    dataset.add(pkg)
    assert pkg.exists

    pkg.insert_property('my-key','my-value')
    pkg2 = client.get(pkg)
    print 'properties =', pkg2.properties
    assert pkg2.id == pkg.id
    assert pkg2.get_property('my-key').data_type == 'string'
    assert pkg2.get_property('my-key').value == 'my-value'

    explicit_ptypes = {
        'my-int1': ('integer', 123123),
        'my-int2': ('integer', '123123'),
        'my-float': ('double', 123.123),
        'my-float2': ('double', '123.123'),
        'my-float3': ('double', '123123'),
        'my-date': ('date', 1488847449697),
        'my-date2': ('date', 1488847449697.123),
        'my-date3': ('date', datetime.datetime.now()),
        'my-string': ('string', 'my-123123'),
        'my-string2': ('string', '123123'),
        'my-string3': ('string', '123123.123'),
        'my-string4': ('string', 'According to plants, humans are blurry.'),
    }
    for key, (ptype,val) in explicit_ptypes.items():
        pkg.insert_property(key, val, data_type=ptype)
        assert pkg.get_property(key).data_type == ptype

    inferred_ptypes = {
        'my-int1': ('integer', 123123),
        'my-int2': ('integer', '123123'),
        'my-float1': ('double', 123.123),
        'my-float2': ('double', '123.123'),
        'my-date': ('date', datetime.datetime.now()),
        'my-string': ('string', 'i123123'),
        'my-string2': ('string', '#1231'),
    }
    for key, (ptype,val) in inferred_ptypes.items():
        pkg.insert_property(key, val)
        prop = pkg.get_property(key)
        assert prop.data_type == ptype

    # remove property
    pkg.remove_property('my-key')
    with pytest.raises(Exception):
        assert pkg.get_property('my-key')

    pkg2 = client.get(pkg.id)
    with pytest.raises(Exception):
        assert pkg2.get_property('my-key')

def test_package_objects(client, client2, dataset):
    """
    Only super-users are allowed to create/modify package sources/files.
    """
    pkg = DataPackage('Some Video', package_type='Video')
    assert not pkg.exists

    # some files (local for now)
    source = File(name='My Source File', s3_key='s3/source', s3_bucket='my-bucket', file_type="JSON", size=1000)
    file   = File(name='My File', s3_key='s3/file', s3_bucket='my-bucket', file_type="CSV", size=1000)
    view   = File(name='My View File', s3_key='s3/view', s3_bucket='my-bucket', file_type="NIFTI", size=1000)

    # get dataset (but as different client)
    dataset2 = client2._api.datasets.get(dataset.id)

    assert dataset2.id == dataset.id
    assert dataset2.exists

    # create package (super-admin user session)
    dataset2.add(pkg)
    assert pkg.exists

    # create package (normal user owns)
    pkg = DataPackage('Some Video', package_type='Video')
    assert not pkg.exists
    dataset.add(pkg)
    assert pkg.exists

    # try doing as normal user - should error
    with pytest.raises(UnauthorizedException):
        pkg.set_sources(source)

    with pytest.raises(UnauthorizedException):
        pkg.set_files(file)

    with pytest.raises(UnauthorizedException):
        pkg.set_view(view)
