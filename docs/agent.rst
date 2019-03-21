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

If the Agent is installed, you can use it for uploading by passing ``use_agent=True`` to ``upload`` and ``append``. This enables features that are not available in older versions of the Python client, such as uploading directories.

Let's get a dataset:

.. code-block:: python

   from blackfynn import Blackfynn
   bf = Blackfynn()
   dataset = bf.get_dataset("My dataset")

As a reminder, you can upload files directly to a dataset or a collection:

.. code-block:: python

   dataset.upload("example_data/gait.csv", "example_data/test_10hz_ms.bfts", use_agent=True)

To upload all files in the ``example_data/`` directory to this dataset, use the following:

.. code-block:: python

   dataset.upload("example_data", use_agent=True)

This will ignore any directories nested in ``example_data/``, but the remaining source files will be uploaded into the dataset and produce a structure like this:

.. code-block:: none

   ├── My dataset (dataset)
   |   ├── gait.csv (package)
   |   ├── ...
   |   └── test_10hz_1ms.bfts (package)

You can also upload an entire directory structure recursively:

.. code-block:: python

   dataset.upload("example_data", recursive=True, use_agent=True)

This creates a new collection called ``example_data``, maintaining the nested structure of the directory on your computer:

.. code-block:: none

   ├── My dataset (dataset)
   |   ├── example_data (collection)
   |       ├── gait.csv (package)
   |       ├── ...
   |       └── test_10hz_1ms.bfts (package)

The ``upload`` method also supports a ``display_progress`` argument that will show progress information from the agent:

.. code-block:: python

   dataset.upload("example_data/test_10hz_1ms.bfts", display_progress=True, use_agent=True)

Visit https://developer.blackfynn.io/agent for more information about using the Blackfynn Agent directly.
