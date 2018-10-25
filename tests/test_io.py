import pytest

from pkg_resources import resource_filename

from blackfynn.models import DataPackage, TimeSeries


def _resource_path(fname):
    return resource_filename('tests.resources', fname)

FILE1 = _resource_path('test-upload.txt')
FILE2 = _resource_path('test-upload-2.txt')
FILE_EMPTY = _resource_path('empty.txt')


@pytest.mark.parametrize('upload_args,n_files', [
    ([FILE1], 1),          # Single file
    ([FILE1, FILE2], 2),   # Multiple files, separate arguments
    ([[FILE1, FILE2]], 2), # Multiple files, single argument list
])
def test_upload(client, dataset, upload_args, n_files):
    """
    Note: ETL will fail since destination will likely be removed
          before being processed.
    """
    # upload file(s) into dataset
    resp = dataset.upload(*upload_args)
    assert len(resp) == n_files

    for r in resp:
        manifest = r[0]['manifest']
        assert manifest['content'] is not None
        assert manifest['type'] == 'upload'

    # try uploading into a DataPackage
    pkg = DataPackage('Rando Thing', package_type='MRI')
    dataset.add(pkg)
    assert pkg.exists

    # should definitely raise an error
    with pytest.raises(Exception):
        pkg.upload(*upload_args)


@pytest.mark.parametrize('append_args,n_files', [
    ([FILE1], 1),          # Single file
    ([FILE1, FILE2], 2),   # Multiple files, separate arguments
    ([[FILE1, FILE2]], 2), # Multiple files, single argument list
])
def test_append(client, dataset, append_args, n_files):
    """
    Note: ETL will fail for append, because... it's a text file.
          But also because the destination will likely be removed
          before the ETL actually places the new node there.
    """
    # TimeSeries package to append into...
    pkg = TimeSeries('Rando Timeseries')
    dataset.add(pkg)
    assert pkg.exists

    # upload/append file into package
    resp = pkg.append_files(*append_args)
    assert len(resp) == n_files

    for r in resp:
        manifest = r[0]['manifest']
        assert manifest['content'] is not None
        assert manifest['type'] == 'append'


def test_progress_for_empty_files(dataset):
    dataset.upload(FILE_EMPTY, display_progress=True)
