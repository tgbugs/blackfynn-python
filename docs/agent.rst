.. _agent:

Using the Blackfynn CLI Agent
=============================


The Blackfynn CLI Agent is a fast native application that integrates with the Python client to provide advanced features.

Visit https://developer.blackfynn.io/agent/ for installation instructions and additional documentation.

See :ref:`preparing` to download the data used in this example.

.. warning::

   Integration between the Python client and the Blackfynn CLI Agent is under active development. Please report any issues to https://github.com/blackfynn/blackfynn-python/issues.


Uploading Directories
^^^^^^^^^^^^^^^^^^^^^

If the Python client detects that the Blackfynn agent is installed, it will attempt to use the Agent for uploading. This enables features that are not available in older versions of the Python client, such as uploading directories.

Let's get a dataset:

.. code-block:: python

   from blackfynn import Blackfynn
   bf = Blackfynn()
   dataset = bf.get_dataset("My dataset")

As a reminder, you can upload files directly to a dataset or a collection:

.. code-block:: python

   dataset.upload("example_data/gait.csv", "example_data/test_10hz_ms.bfts")

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

Visit https://developer.blackfynn.io/agent for more information about using the Blackfynn Agent directly.
