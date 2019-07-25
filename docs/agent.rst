.. _agent:

Uploading with the Blackfynn Agent
==================================


The Blackfynn CLI Agent is a fast native application that integrates with the Python client to provide advanced features.

Visit https://developer.blackfynn.io/agent/ for installation instructions and additional documentation.

See :ref:`preparing` to download the data used in this example.

.. warning::

   By default, uploads through the Python client are handled by the Blackfynn Agent.
   If the agent is not installed then the legacy uploader can be used by passing
   ``use_agent=False`` to the ``.upload()`` and ``.append_files()`` methods. Some
   features such as uploading directories are unsupported by the legacy uploader.


Uploading Files
^^^^^^^^^^^^^^^

You can upload files directly to a dataset or a collection:

.. code-block:: python

   from blackfynn import Blackfynn
   bf = Blackfynn()

   # upload into the top level of the dataset
   dataset = bf.get_dataset("My dataset")
   dataset.upload("example_data/gait.csv", "example_data/test_10hz_ms.bfts")

   # upload into a collection
   collection = dataset.create_collection("my data folder")
   collection.upload("example_data/test.edf")


Appending to a Time Series
^^^^^^^^^^^^^^^^^^^^^^^^^^
Append files to a timeseries with the ``.append_files()`` method:

.. code-block:: python

   from blackfynn import Blackfynn
   from blackfynn.models import TimeSeries
   bf = Blackfynn()
   dataset = bf.get_dataset("My dataset")

   timeseries = TimeSeries("My TimeSeries")
   dataset.add(timeseries)
   timeseries.append_files("example_data/T2.nii.gz")


Uploading Directories
^^^^^^^^^^^^^^^^^^^^^

Let's first get a dataset:

.. code-block:: python

   from blackfynn import Blackfynn
   bf = Blackfynn()
   dataset = bf.get_dataset("My dataset")


To upload all files in the ``example_data/`` directory to this dataset, use the following:

.. code-block:: python

   dataset.upload("example_data")

This will ignore any directories nested in ``example_data/``, but the remaining source files will be uploaded into the dataset and produce a structure like this:

.. code-block:: none

   ├── My dataset (dataset)
   |   ├── gait.csv (package)
   |   ├── ...
   |   └── test_10hz_1ms.bfts (package)

You can also upload an entire directory structure recursively:

.. code-block:: python

   dataset.upload("example_data", recursive=True)

This creates a new collection called ``example_data``, maintaining the nested structure of the directory on your computer:

.. code-block:: none

   ├── My dataset (dataset)
   |   ├── example_data (collection)
   |       ├── gait.csv (package)
   |       ├── ...
   |       └── test_10hz_1ms.bfts (package)

The ``upload`` method also supports a ``display_progress`` argument that will show progress information from the agent:

.. code-block:: python

   dataset.upload("example_data/test_10hz_1ms.bfts", display_progress=True)


.. note::

   The Blackfynn Agent has its own command line interface with much more functionality than what is shown here.
   Visit https://developer.blackfynn.io/agent for more information.
