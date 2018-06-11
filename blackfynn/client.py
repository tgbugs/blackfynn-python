# -*- coding: utf-8 -*-

# blackfynn-specific
from blackfynn import Settings
from blackfynn.models import Dataset
from blackfynn.api.transfers import IOAPI
from blackfynn.api.compute import ComputeAPI
from blackfynn.api.ledger import LedgerAPI
from blackfynn.api.user import UserAPI
from blackfynn.api.concepts import (
    ModelsAPI, RecordsAPI, ModelRelationshipsAPI, ModelRelationshipInstancesAPI
)
from blackfynn.api.timeseries import TimeSeriesAPI
from blackfynn.base import ClientSession
from blackfynn.api.core import (
    CoreAPI, SecurityAPI, OrganizationsAPI, SearchAPI
)
from blackfynn.api.data import (
    DatasetsAPI, PackagesAPI, FilesAPI, DataAPI, TabularAPI
)

class Blackfynn(object):
    """
    The client provides an interface to the Blackfynn platform, giving you the
    ability to retrieve, add, and manipulate data.

    Args:
        profile (str, optional): Preferred profile to use
        api_token (str, optional): Preferred api token to use
        api_secret (str, optional): Preferred api secret to use
        host (str, optional): Preferred host to use
        streaming_host (str, optional): Preferred streaming host to use
        concepts_host (str, optional): Preferred concepts service host to use
        env_override (bool, optional): Should environment variables override settings
        **overrides (dict, optional): Settings to override

    Examples:
        Load the client library and initialize::

            from blackfynn import Blackfynn

            # note: no API token needed if environment variables are set
            bf = Blackfynn()

        Retrieve/modify items on the platform::

            # get some dataset
            ds = bf.get_dataset('my dataset')
            print "Dataset {} has ID={}".format(ds.name, ds.id)

            # list all first-level items in dataset
            for item in ds:
                print "Item: {}".format(item)

            # grab some data package
            pkg = bf.get("N:package:1234-1234-1234-1234")

            # modify a package's name
            pkg.name = "New Package Name"

            # add some property
            pkg.set_property('Room', 'ICU-123')

            # sync changes
            pkg.update()


    Note:
        To initialize your ``Blackfynn`` client without passing any arguments,
        ensure that your ``BLACKFYNN_API_TOKEN`` and ``BLACKFYNN_API_SECRET`` environment variables
        are properly set.

    """
    def __init__(self, profile=None, api_token=None, api_secret=None, host=None, streaming_host=None, concepts_host=None, env_override=True, **overrides):

        overrides.update({ k: v for k, v in {
            'api_token': api_token,
            'api_secret': api_secret,
            'api_host': host,
            'api_streaming_host': streaming_host,
            'api_concepts_host': concepts_host,
            }.items() if v != None })
        self.settings = Settings(profile, overrides, env_override)

        if self.settings.api_token  is None: raise Exception('Error: No API token found. Cannot connect to Blackfynn.')
        if self.settings.api_secret is None: raise Exception('Error: No API secret found. Cannot connect to Blackfynn.')

        # direct interface to REST API.
        self._api = ClientSession(self.settings)

        # account
        try:
            self._api.authenticate()
        except Exception as e:
            raise e

        self._api.register(
            CoreAPI,
            OrganizationsAPI,
            DatasetsAPI,
            FilesAPI,
            DataAPI,
            PackagesAPI,
            TimeSeriesAPI,
            TabularAPI,
            SecurityAPI,
            ComputeAPI,
            SearchAPI,
            IOAPI,
            LedgerAPI,
            UserAPI,
            ModelsAPI,
            RecordsAPI,
            ModelRelationshipsAPI,
            ModelRelationshipInstancesAPI
        )

        self._api._context = self._api.organizations.get(self._api._organization)


    @property
    def context(self):
        """
        The current organizational context of the active client.
        """
        return self._api._context

    @property
    def profile(self):
        """
        The profile of the current active user.
        """
        return self._api.profile

    def organizations(self):
        """
        Return all organizations for user.
        """
        return self._api.organizations.get_all()

    def datasets(self):
        """
        Return all datasets for user for an organization (current context).
        """
        self._check_context()
        return self.context.datasets

    def get(self, id, update=True):
        """
        Get any DataPackage or Collection object by ID.

        Args:
            id (str): The ID of the Blackfynn object.

        Returns:
            Object of type/subtype ``DataPackage`` or ``Collection``.
        """
        try:
            return self._api.core.get(id, update=update)
        except:
            print("Unable to retrieve object"\
                  "\n\nAcceptable objects for get() are:"\
                  "\n - DataPackages"\
                  "\n - Collections"\
                  "\n\nUse get_dataset() if trying to retrieve a dataset")

    def create(self, thing):
        """
        Create a object on the platform.
        """
        return self._api.core.create(thing)

    def create_dataset(self, name):
        """
        Create a dataset under the active organization.

        Args:
            name (str): The name of the to-be-created dataset

        Returns:
            The created ``Dataset`` object

        """
        self._check_context()
        return self._api.datasets.create(Dataset(name))

    def get_dataset(self, name_or_id):
        """
        Get Dataset by name or ID.

        Args:
            name_or_id (str): the name or the ID of the dataset

        Note:
            When using name, this method gnores case, spaces, hyphens,
            and underscores such that these are equivelent:

              - "My Dataset"
              - "My-dataset"
              - "mydataset"
              - "my_DataSet"
              - "mYdata SET"

        """
        try:
            return self._api.datasets.get(name_or_id)
        except:
            pass

        result = self._api.datasets.get_by_name_or_id(name_or_id)
        if result is None:
            raise Exception("No dataset matching name or ID '{}'.".format(name_or_id))
        return result

    def update(self, thing):
        """
        Update an item on the platform.

        Args:
            thing (object or str): the ID or object to update

        Example::

            my_eeg = bf.get('N:package:1234-1234-1234-1234')
            my_eeg.name = "New EEG Name"
            bf.update(my_eeg)

        Note:
            You can also call update directly on objects, for example::

                my_eeg.name = "New EEG Name"
                my_eeg.update()

        """
        return self._api.core.update(thing)

    def delete(self, *things):
        """
        Deletes items from the platform.

        Args:
            things (list): ID or object of items to delete

        Example::

            # delete a package by passing in object
            bf.delete(my_package)

            # delete a package by ID
            bf.delete('N:package:1234-1234-1234-1235')

            # delete many things (mix of IDs and objects)
            bf.delete(my_package, 'N:collection:1234-1234-1234-1235', another_collection)

        Note:
            When deleting ``Collection``, all child items will also be deleted.

        """
        return self._api.core.delete(*things)

    def move(self, destination, *things):
        """
        Moves item(s) from their current location to the destination.

        Args:
            destination (object or str): The ID of the destination. This must be of type
                type ``Collection`` or ``None``. If destination is ``None``, ``things``
                will be moved to the top of their containing ``Dataset``
            things (list): the IDs or objects to move.

        """
        r = self._api.data.move(destination, *things)

    def search(self, query, max_results=10):
        """
        Find an object on the platform.

        Args:
            query (str): query string to perform search.
            max_results (int, optional): the number of results to return

        Example::

            # find some items belonging to patient123
            for result in  bf.search('patient123'):
                print "found:", result

        """
        return self._api.search.query(query, max_results=max_results)

    def _check_context(self):
        if self.context is None:
            raise Exception('Must set context before executing method.')

    def __repr__(self):
        return "<Blackfynn user='{}' organization='{}'>".format(self.profile.email, self.context.name)
