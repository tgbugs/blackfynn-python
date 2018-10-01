Working with Tabular Data
===============================

In this section of the tutorial, we will show how to work with tabular data using the Blackfynn
Python client. We recommend you that, in order to get the most out of this tutorial, you
look into the :ref:`Working with the data catalog` tutorial first.

Through this tutorial, we will show some examples that will show some of
the methods that the Blackfynn python client offers for working with
tabular data. You can find the details about the supported file formants
in the :ref:`file formats <Supported File Formats>` section.

We will be using the demographics and disease severity information file
for the Gait in Parkinson's Disease database. We have included this
``CSV`` file in the tutorial's data directory. However, this dataset
is publicly available and can also be obtained from the
`Physionet <https://physionet.org/pn3/gaitpdb/>`_ website.

Tabular Data Basics
^^^^^^^^^^^^^^^^^^^^^

We will start by uploading the tabular data to the platform and
downloading it in the standard format in order to demonstrate some of
the download parameters.

.. code-block:: python
   :linenos:

    # import blackfynn
    from blackfynn import Blackfynn

    # create a client instance
    bf = Blackfynn()

    # create a dataset in the platform to save our tabular data and get dataset object
    ds = bf.create_dataset('Tabular Dataset')

.. code-block:: python
   :linenos:

    # upload file to platform
    ds.upload('example_data/gait.csv');

    for item in ds:
        print("Type:", item.type, "|", "Name:", item.name, "|" , "ID:", item.id)

.. note::
     If you are uploading large files, you might not see all of your
     packages right away. You might have to wait for a few seconds.
     To check if your package is ready, you can get the package's state
     through the ``state`` attribute of the package's object. If the
     package is done uploading and ready, ``pkg.state`` should return
     ``READY``.

.. code-block:: console
   :linenos:

    Type: Tabular | Name: gait | ID: N:package:35716bd1-dde0-4c09-b7ab-04a63b0ac29f


We see that our package has been successfully uploaded to the Blackfynn
platform and that it has been assigned the type ``Tabular``.

Downloading Tabular Data
^^^^^^^^^^^^^^^^^^^^^^^^^

So far, we have uploaded a ``CSV`` file and created a ``Tabular``
package in the Balckfynn platform. We will now demonstrate partial and
full download of the data through the ``get_data()`` method.

First, we will obtain the entire dataset.

.. code-block:: python
   :linenos:

    # get the package object through its ID
    tb = bf.get('N:package:35716bd1-dde0-4c09-b7ab-04a63b0ac29f')

    # get all the data
    data = tb.get_data()

    # print rows and column information for the data
    print("Data has {} rows and {} columns".format(len(data.index),len(data.columns)))
    print("First Index: {}".format(data.index[0]))
    print("Last Index: {}".format(data.index[len(data.index)-1]))

    # print the column names for the data
    print("\nColumns in the data:")
    print(" | ".join(data.columns))

    data

.. code-block:: console

    Data has 166 rows and 16 columns
    First Index: 0
    Last Index: 165

    Columns in the data:
    id | subject_type | speed_01 | speed_10 | reference | v_lastmodified_epoch | v_status | v_uuid | gender | age | height | weight | hoehnyahr | updrs | updrsm | tuag

.. csv-table:: data
   :header-rows: 1
   :widths: 5 5 5 5 5 5 5 5 5 5 5 5 5 5 5 5 5
   :file: ../static/files/gaitParkinsons_full.csv


We see that the data was read into a dataframe, and that we read all the
166 rows and 16 columns of data. However, it is also possible to read
the data partially, which is useful in the prescence of large datasets.

.. code-block:: python
   :linenos:

    # get only the first 5 rows
    data = tb.get_data(limit=5)

    # print rows and column information for the data
    print("Data has {} rows and {} columns".format(len(data.index),len(data.columns)))

    data


.. code-block:: console

    Data has 5 rows and 16 columns

.. csv-table:: data
   :header-rows: 1
   :widths: 5 5 5 5 5 5 5 5 5 5 5 5 5 5 5 5 5
   :file: ../static/files/gaitParkinsons_0_to_4.csv

We see that in this case we only got the first 5 rows of data, because
we specified that number of rows using the ``limit`` parameter for
``get_data()``.

What if we wanted to only take rows 20 through 30? This would be
possible by specifying a ``limit`` of 10, to get 10 rows, and an
``offset`` of 20, to start getting the data in the 20th row. This is
demonstrated as follows.

.. code-block:: python
   :linenos:

    # get only the first 5 rows
    data = tb.get_data(limit=10, offset=20)

    # print rows and column information for the data
    print("Data has {} rows and {} columns".format(len(data.index),len(data.columns)))

    data

.. code-block:: console

    Data has 10 rows and 16 columns

.. csv-table:: data
   :header-rows: 1
   :widths: 5 5 5 5 5 5 5 5 5 5 5 5 5 5 5 5 5
   :file: ../static/files/gaitParkinsons_20_to_30.csv

.. note::
   The maximum default value for ``limit`` is 1000. In order to get larger chunks of data we
   recomend using the ``get_data_iter()`` method, which gets the gata in an iterative manner.
   The method is fully described in the :ref:`tabular <Tabular>` section of the Data Models page.

Analyzing the Data
^^^^^^^^^^^^^^^^^^^

As we have seen, the python client allows to get the data in a format
that is flexible and easy to use. We now show a very simple example for
plotting the data that we already have.

.. note::
   In order to plot the data as shown in the following part of the tutorial,
   you need to have installed `matplotlib <https://matplotlib.org/users/installing.html>`_.

.. code-block:: python
   :linenos:

    import matplotlib.pyplot as plt
    import pandas as pd
    import numpy as np

    # get all the data
    data = tb.get_data()

    # give the index a name (ind)
    data['ind']= data.index

    # define x and y variables (and get rid of undefined entries)
    x=data['updrs'].fillna(0)
    y=data['updrsm'].fillna(0)
    plt.scatter(x,y, color='c')

    z = np.polyfit(x, y, 1)
    p = np.poly1d(z)

    plt.plot(x,p(x),"r--")

    # adjust axes of plot and add labels
    axes = plt.gca()
    axes.set_title('UPDRSM vs. UPDRS')
    axes.set_xlabel('UPDRS'); axes.set_ylabel('UPDRSM')

    plt.show()

.. image:: ../static/tabular_15_0.png

.. note::
   The reason for presenting the example above is to illustrate how simple
   it can be to work with the downloaded dataframe. This is just
   a very easy example to get you stated in the analysis and exploration of
   your tabular datasets.
