Working with the data catalog
===============================

This section provides examples that show how to interact with :ref:`collections <Collection>`, :ref:`datasets <Dataset>` and :ref:`packages <Data Package>`.

All of the short scripts presented below are fully commented to properly explain each step.

Catalog Basics
^^^^^^^^^^^^^^

Connect, and print some basic account and dataset information.

.. code-block:: python
   :linenos:

   # create a client instance
   from blackfynn import Blackfynn

   # create a client instance
   bf = Blackfynn()

   # print account information
   print("email =", bf.profile.email)

   # print current context (i.e. organization)
   print("organization = ", bf.context.name)

   # which datasets can we access?
   print("my datasets: ")
   for ds in bf.datasets():
       print(ds)

Example response:

.. code-block:: console

    email = support@blackfynn.com

    organization = Demo Organization

    my datasets:
     <Dataset name='Test Data' id='N:dataset:d2a30fbf-4c13-4188-9f1a-89e89967c03b'>
     <Dataset name='My Research Study' id='N:dataset:8664332c-0836-4d22-bda4-b27fea9659b1'>

We can see that ``bf.datasets()`` gets all of the datasets available to the current user.
In this example, the only datasets available are ``Test Data`` and ``My Research Study``.

Creating new data
^^^^^^^^^^^^^^^^^^

We will create a new dataset, upload a file, then read the uploaded file using the client.

.. code-block:: python
   :linenos:

    # create a dataset in the current organization.
    # The name for our new dataset will be 'New Dataset'
    ds = bf.create_dataset('New Dataset')

    print(ds)

Example response:

.. code-block:: console

    <Dataset name='New Dataset' id='N:dataset:ebc05784-e6d0-4c2b-975d-72d3fdd5facc'>

We can see that an ID string ``N:dataset:ebc05784-e6d0-4c2b-975d-72d3fdd5facc`` has
been assigned to our created dataset as an unique identifier for the object.

.. code-block:: python
   :linenos:

    # get the dataset
    #
    ds = bf.get_dataset('New Dataset')

    # add a file to the newly created dataset.
    # this line will upload the timeseries file
    # "test.edf" to out dataset
    #
    ds.upload('example_data/test.edf');

When we upload a file to a dataset, a package with the same name of
the uploaded file and an assigned unique ID is created in the
Blackfynn platform. In this case, a timeseries file of name 'test' is
created in our ``New Dataset``.
Because the name ``New Dataset`` is not very informative, we will now
change the name of the dataset in the platform.

.. code-block:: python
   :linenos:

    # change name of the dataset
    #
    new_name = 'BF Tutorial'
    ds.name = new_name
    ds.update()

    bf.datasets()

Example Response:

.. code-block:: console
    :emphasize-lines: 3

    [<Dataset name='Test Data' id='N:dataset:d2a30fbf-4c13-4188-9f1a-89e89967c03b'>,
    <Dataset name='My Research Study' id='N:dataset:8664332c-0836-4d22-bda4-b27fea9659b1'>,
    <Dataset name='BF Tutorial' id='N:dataset:ebc05784-e6d0-4c2b-975d-72d3fdd5facc'>]

We can see that the dataset that we created, previously called
``New Dataset``, is now called ``BF Tutorial``. Note that while the name
of the dataset has changed, its unique ID remains the same.

As an excercise, we will upload all of the non-tabular data that is
available in the test set to a ``Collection`` called
"original collection 1" and all of the available tabular data to another
collection called "original collection 2". We will then move all of the
content in these collections to a third collection called
"final collection". These collections will be created inside a
dataset called "Practice Dataset".

Our first step will be to create the dataset and collections that we
will be working with.

.. code-block:: python
   :linenos:

    # import collections
    from blackfynn import Collection

    # create and get a new dataset
    ds=bf.create_dataset("Practice Dataset")

    # create new collections
    ds.create_collection("original collection 2")
    ds.create_collection("original collection 1")
    ds.create_collection("final collection")

    print("Contents for", ds.name)
    for item in ds:
        print(item)

.. code-block:: console

    Contents for Practice Dataset
    <Collection name='original collection 1' id='N:collection:243062ce-fdbf-4331-8c21-bc2d09b0089e'>
    <Collection name='original collection 2' id='N:collection:3a242008-5875-4b38-b651-ed6ffdca0e80'>
    <Collection name='final collection' id='N:collection:0975ef4b-c851-417e-bc6f-c2f81a78a627'>


We have now created the dataset and collections. We can see that the
Database that we created contains three collections:
``original collection 2``, ``original collection 1`` and
``final collection``.

We will now use the Collection ID's in the dataset to get the collection
objects that we will be working with. Then, we can upload the files to
their corresponding collections.

.. code-block:: python
   :linenos:

    col1 = bf.get('N:collection:243062ce-fdbf-4331-8c21-bc2d09b0089e')
    col2 = ds[1]
    col3 = ds[2]

    # add data to the collections
    #
    col1.upload('example_data/table1.csv',\
                'example_data/table2.csv',\
                'example_data/table3.csv')

    col2.upload('example_data/testData.nev',\
                'example_data/testData.ns2', \
                'example_data/T2.nii.gz',\
        'example_data/blackfynn.pdf',\
        'example_data/small_region.svs')

.. note::
   We used the ``get()`` method to get col1 for illustrative purposes. However,
   since we already got the dataset, we can just accesss the package objects throguh
   indices. For more information about the ``get()`` method you can visit the
   :ref:`client interface page <Blackfynn Client Interface>`.

At this point, we have uploaded the data to their respective
collections. We can see all of the content of the dataset by using the
``print_tree()`` method.

.. note::
  If you are uploading large files, you might not see everything with ``print_tree()`` right away.
  You might have to wait for a few seconds. To check if your package is ready, you can get the
  package's state through the ``state`` attribute of the package's object. If the package is done
  uploading and ready, ``pkg.state`` should return ``READY``.

.. code-block:: python
   :linenos:

    # print everything under "Practice Dataset"
    ds.update()
    ds.print_tree()


.. code-block:: console
   :emphasize-lines: 10

    <Dataset name='Practice Dataset' id='N:dataset:aaaace74-b27a-4069-8b0b-5a102c4dcecb'>
      <Collection name='original collection 1' id='N:collection:243062ce-fdbf-4331-8c21-bc2d09b0089e'>
        <Tabular name='table1' id='N:package:233776aa-a8c0-4d81-9514-8d5ef6a402f6'>
        <Tabular name='table2' id='N:package:e6be4926-0aaa-4997-8634-5a00a7a92583'>
        <Tabular name='table3' id='N:package:6bdd79d3-1de2-415d-a2b4-dce03c6ba895'>
      <Collection name='original collection 2' id='N:collection:3a242008-5875-4b38-b651-ed6ffdca0e80'>
        <DataPackage name='small_region' id='N:package:25eb1f60-7593-4cc7-9bff-aab3b2859f32'>
        <DataPackage name='T2' id='N:package:cd6784b6-ba5d-4cc5-8a86-93f279b2832b'>
        <DataPackage name='blackfynn' id='N:package:7548dbd9-0886-4ec5-8262-e7ab6e8f212f'>
        <TimeSeries name='testData' id='N:package:5c7fd669-4333-48c8-ac5a-9f549a3efc4d'>
      <Collection name='final collection' id='N:collection:0975ef4b-c851-417e-bc6f-c2f81a78a627'>

The output shows that the uploaded packages have been created in our
"Other DS" dataset. Note that the ``testData.*`` (highlighted) files were uploaded
as one package. This happens because, since the files belong to the same
session (with the NEV file containing extracellular spike information,
stimulation waveformas and input events, and the NSx file containing
continuously sampled analog data) Blackynn associates both files as one
package. See the :ref:`Client Interface` ``upload()`` section for more information about data
formats that rely on multiple files.

Deleting and moving items
^^^^^^^^^^^^^^^^^^^^^^^^^^

.. code-block:: python
   :linenos:

    # move al content to "final collection"
    for item in [col1.items, col2.items]:
        for package in item:
            bf.move(col3, package.id)

    # remove empty collections
    col1.delete()
    col2.delete()

    # print content of "Practice Dataset"
    ds.update()
    ds.print_tree()

.. code-block:: console

    <Dataset name='Practice Dataset' id='N:dataset:aaaace74-b27a-4069-8b0b-5a102c4dcecb'>
      <Collection name='final collection' id='N:collection:0975ef4b-c851-417e-bc6f-c2f81a78a627'>
        <Tabular name='table1' id='N:package:233776aa-a8c0-4d81-9514-8d5ef6a402f6'>
        <Tabular name='table2' id='N:package:e6be4926-0aaa-4997-8634-5a00a7a92583'>
        <Tabular name='table3' id='N:package:6bdd79d3-1de2-415d-a2b4-dce03c6ba895'>
        <DataPackage name='small_region' id='N:package:25eb1f60-7593-4cc7-9bff-aab3b2859f32'>
        <DataPackage name='T2' id='N:package:cd6784b6-ba5d-4cc5-8a86-93f279b2832b'>
        <DataPackage name='blackfynn' id='N:package:7548dbd9-0886-4ec5-8262-e7ab6e8f212f'>
        <TimeSeries name='testData' id='N:package:5c7fd669-4333-48c8-ac5a-9f549a3efc4d'>

We have now reviewed the main functions that revolve around interacting
with the Blackfynn data catalog.

.. note::
    It is important to note that, for safety,
    Datasets cannot be deleted from the clients. In this sense, if you would
    like to delete a Dataset, you can go to the web UI of the Blackfynn
    platform, go into the Dataset that you wish to delete, click on the
    information icon in the top right corner, click on ``Edit Settings`` and
    select ``Delete this dataset``.
