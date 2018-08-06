Getting Started in Python
=========================

Python client and command line tool for Blackfynn.

Installation
------------

.. code:: python

      pip install -U blackfynn

.. important:: 

    In order to connect to Blackfynn using the *any* client, first you must `Generate API token & secret and enable in client <http://help.blackfynn.com/blackfynn-developer-tools/overview/creating-an-api-key-for-the-blackfynn-clients>`_.

.. note::

    We currently only support and test on *Python v2.7*. This will be expanded in the
    near future.


Basic Usage
--------------

Import and Initialize
~~~~~~~~~~~~~~~~~~~~~~

.. code:: python

    from blackfynn import Blackfynn

    bf = Blackfynn()

This will use your *default profile* to establish a connection. Alternatively, you may want to specify a profile explicitly:

.. code:: python

    bf = Blackfynn('my_profile')

Where ``my_profile`` is an existing profile. 

.. note::
    See :ref:`Create connection profile` for creating and managing connection profiles.

Basic Operations
~~~~~~~~~~~~~~~~~~~~~~

Get your datasets::

    # print your available datasets
    for ds in bf.datasets():
        print " Found a dataset: ", ds.name

    # grab some dataset by name
    ds1 = bf.get_dataset('my dataset 1')

    # list items inside dataset (first level)
    print ds1.items

Upload some files into your dataset::

    ds1.upload('/path/to/data.pdf')

Get a data package::

    # use ID to get a package
    pkg = bf.get('N:package:1234-1234-1234-1235')

Rename it & add some properties::

    pkg.name = "My new package name"
    pkg.set_property('Temperature', 83.0)
    pkg.update()


Uploading files
----------------

.. note::
  You must upload files into a ``Dataset`` or ``Collection``.

You can upload using the ``.upload()`` methods provided on ``Dataset`` and ``Collection`` objects::

    # upload a file into a dataset (ds)
    ds.upload('/path/to/my_data.nii.gz')

Retrieving data
----------------

Let's say you grab a ``TimeSeries`` package::

    ts = bf.get('N:package:your-timeseries-id')

You can get first minute of data in 1-second chunks::

    for chunk in ts.get_data_iter(chunk_size='1s', length='1m'):
        # do something with data (pandas Dataframe)
        print "Mean values =", chunk.mean()

You can do the same thing for a single channel::

    channel = ts.channels[0]
    for chunk in channel.get_data_iter(chunk_size='5s', length='10m'):
        # do something with data (pandas Series)
        print "Max value =", chunk.max()
