Working with Tabular Data
===============================

In this section of the tutorial, we will show how to work with tabular data
using the Blackfynn Python client. We recommend that, in order to get the
most out of this tutorial, you look into the :ref:`Working with the data
catalog` tutorial first. You can find the details about supported file formats
in the `Supported File Formats
<http://help.blackfynn.com/general-information/supported-file-formats>`_
section.

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

    # upload file to platform
    ds.upload('example_data/gait.csv')

    print(ds.items)

.. code-block:: console
   :linenos:

   [<Tabular name='gait' id='N:package:35716bd1-dde0-4c09-b7ab-04a63b0ac29f']

We see that our package has been successfully uploaded to the Blackfynn
platform and that it has been assigned the type ``Tabular``.

.. note::
     If you are uploading large files, you might not see all of your
     packages right away. You might have to wait for a few seconds.
     To check if your package is ready, you can get the package's state
     through the ``state`` attribute of the package's object. If the
     package is done uploading and ready, ``pkg.state`` should return
     ``READY``.

Downloading Tabular Data
^^^^^^^^^^^^^^^^^^^^^^^^^

So far, we have uploaded a ``CSV`` file and created a ``Tabular``
package in the Balckfynn platform. We will now demonstrate partial and
full download of the data through the ``get_data()`` method.

First, we will obtain the entire dataset.

.. code-block:: python
   :linenos:

    # get the tabular package from the dataset
    tb = ds.items[0]

    # get all the data
    data = tb.get_data()

    print('Columns in the data:')
    print(list(data.columns))

    print(data)

.. code-block:: console

    Columns in the data:
    ['id', 'subject_type', 'speed_01', 'speed_10', 'reference', 'v_lastmodified_epoch', 'v_status', 'v_uuid', 'gender', 'age', 'height', 'weight', 'hoehnyahr', 'updrs', 'updrsm', 'tuag']

    id                             subject_type  ...    updrsm   tuag
    0    GaPt03  Parkinsons Patient, Gait study  ...      10.0  36.34
    1    GaPt04  Parkinsons Patient, Gait study  ...       8.0  11.00
    2    GaPt05  Parkinsons Patient, Gait study  ...       5.0  14.50
    3    GaPt06  Parkinsons Patient, Gait study  ...      13.0  10.47
    4    GaPt07  Parkinsons Patient, Gait study  ...      22.0  18.34
    5    GaPt08  Parkinsons Patient, Gait study  ...       8.0  10.11
    6    GaPt09  Parkinsons Patient, Gait study  ...      17.0  12.70
    7    GaPt12  Parkinsons Patient, Gait study  ...       7.0   8.37
    8    GaPt13  Parkinsons Patient, Gait study  ...      21.0  15.51
    9    GaPt14  Parkinsons Patient, Gait study  ...      19.0    NaN
    ...
    159  SiCo24     Control Patient, Gait study  ...       NaN  11.05
    160  SiCo25     Control Patient, Gait study  ...       NaN   9.16
    161  SiCo26     Control Patient, Gait study  ...       NaN   9.20
    162  SiCo27     Control Patient, Gait study  ...       NaN  12.52
    163  SiCo28     Control Patient, Gait study  ...       NaN  12.65
    164  SiCo29     Control Patient, Gait study  ...       NaN  11.41
    165  SiCo30     Control Patient, Gait study  ...       NaN   8.68

    [166 rows x 16 columns]


We see that ``get_data()`` returns a dataframe, and that we read all the
166 rows and 16 columns of data. However, it is also possible to read
the data partially, which is useful in the presence of large datasets.

.. code-block:: python
   :linenos:

    # get only the first 5 rows
    data = tb.get_data(limit=5)
    print(data)

.. code-block:: console

    id                           subject_type  ...    updrsm   tuag
    0  GaPt03  Parkinsons Patient, Gait study  ...      10.0  36.34
    1  GaPt04  Parkinsons Patient, Gait study  ...       8.0  11.00
    2  GaPt05  Parkinsons Patient, Gait study  ...       5.0  14.50
    3  GaPt06  Parkinsons Patient, Gait study  ...      13.0  10.47
    4  GaPt07  Parkinsons Patient, Gait study  ...      22.0  18.34

    [5 rows x 16 columns]

We see that in this case we only got the first 5 rows of data, because
we specified that number of rows using the ``limit`` parameter for
``get_data()``.

What if we wanted to only take rows 20 through 30? This would be
possible by specifying a ``limit`` of 10, to get 10 rows, and an
``offset`` of 20, to start getting the data in the 20th row. This is
demonstrated as follows.

.. code-block:: python
   :linenos:

    # get rows 20-30
    data = tb.get_data(limit=10, offset=20)
    print(data)

.. code-block:: console

    id                           subject_type  ...    updrsm   tuag
    0  GaPt24  Parkinsons Patient, Gait study  ...      15.0  11.42
    1  GaPt25  Parkinsons Patient, Gait study  ...      18.0  15.22
    2  GaPt26  Parkinsons Patient, Gait study  ...       5.0   7.27
    3  GaPt27  Parkinsons Patient, Gait study  ...      10.0   7.88
    4  GaPt28  Parkinsons Patient, Gait study  ...      29.0  13.02
    5  GaPt29  Parkinsons Patient, Gait study  ...      16.0  10.16
    6  GaPt30  Parkinsons Patient, Gait study  ...      12.0   9.91
    7  GaPt31  Parkinsons Patient, Gait study  ...      13.0  12.60
    8  GaPt32  Parkinsons Patient, Gait study  ...      24.0  11.22
    9  GaPt33  Parkinsons Patient, Gait study  ...      31.0  11.97

    [10 rows x 16 columns]

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
    data['ind'] = data.index

    # define x and y variables (and get rid of undefined entries)
    x = data['updrs'].fillna(0)
    y = data['updrsm'].fillna(0)
    plt.scatter(x, y, color='c')

    z = np.polyfit(x, y, 1)
    p = np.poly1d(z)

    plt.plot(x, p(x), "r--")

    # adjust axes of plot and add labels
    axes = plt.gca()
    axes.set_title('UPDRSM vs. UPDRS')
    axes.set_xlabel('UPDRS')
    axes.set_ylabel('UPDRSM')

    plt.show()

.. image:: ../static/tabular_15_0.png

.. note::
   The reason for presenting the example above is to illustrate how simple
   it can be to work with the downloaded dataframe. This is just
   a very easy example to get you started in the analysis and exploration of
   your tabular datasets.
