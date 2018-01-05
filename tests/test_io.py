import os
import pytest

from blackfynn.models import DataPackage, TimeSeries

def test_upload(client, dataset):
    """
    Note: ETL will fail since destination will likely be removed
          before being processed. 
    """
    srcdir = os.path.dirname(__file__)
    file1 = os.path.join(srcdir, 'test-upload.txt')
    files = [file1]

    # upload a file into dataset
    r = dataset.upload(*files)
    assert len(r['files']) == len(files)
    assert 'destination' not in r
    assert r['dataset'] == dataset.id
    assert r['appendToPackage'] == False

    # try uploading into a DataPackage
    pkg = DataPackage('Rando Thing', package_type='MRI')
    dataset.add(pkg)
    assert pkg.exists

    # should definitely raise an error
    with pytest.raises(Exception):
        pkg.upload(*files)


def test_append(client, dataset):
    """
    Note: ETL will fail for append, because... it's a text file.
          But also because the destination will likely be removed
          before the ETL actually places the new node there.
    """
    srcdir = os.path.dirname(__file__)
    file1 = os.path.join(srcdir, 'test-upload.txt')

    # TimeSeries package to append into...
    pkg = TimeSeries('Rando Timeseries')
    dataset.add(pkg)
    assert pkg.exists

    # upload/append file into package
    r = pkg.append_files(file1)
    assert len(r['files']) == 1
    assert r['destination'] == pkg.id
    assert r['dataset'] == pkg.dataset
    assert r['appendToPackage'] == True
