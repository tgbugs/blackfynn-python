blackfynn-python
================

Python client and command line tool for Blackfynn.

Installation
------------

To install the light client, simply

.. code:: bash

    pip install -U blackfynn

For access to timeseries and tabular data streams

.. code:: bash

    pip install -U blackfynn[data]


See `install notes <INSTALL.rst>`_ for more details.

Documentation and Help
----------------------

Client and command line documentation can be found on `Blackfynn’s
documentation website`_.

.. _Blackfynn’s documentation website: http://docs.blackfynn.io/platform/clients/index.html#python-client

Development
-----------

Note that this package has two installation options: *light* client and *data* client.

The *data* client includes additional dependencies that should only be imported from `extensions.py`. When using these dependencies in your methods, use the `check_extension` function at the start of your function or decorate with the `require_extension` decorator to guard against unexpected import errors.
