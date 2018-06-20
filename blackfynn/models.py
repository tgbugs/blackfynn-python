# -*- coding: utf-8 -*-

import os
import re
import pytz
import dateutil
import datetime
import requests
import numpy as np
import pandas as pd

from uuid import uuid4
from blackfynn.utils import (
    infer_epoch, get_data_type, value_as_type, usecs_to_datetime
)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Helpers
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def get_package_class(data):
    """
    Determines package type and returns appropriate class.
    """
    content = data.get('content', data)
    if 'packageType' not in content:
        p = Dataset
    else:
        ptype = content['packageType'].lower()
        if ptype == 'collection':
            p = Collection
        elif ptype == 'timeseries':
            p = TimeSeries
        elif ptype == 'tabular':
            p = Tabular
        elif ptype == 'dataset':
            p = Dataset
        else:
            p = DataPackage

    return p

def _update_self(self, updated):
    if self.id != updated.id:
        raise Exception("cannot update {} with {}".format(self, updated))

    self.__dict__.update(updated.__dict__)

    return self

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Basics
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Property(object):
    """
    Property of a blackfynn object.

    Args:
        key (str): the key of the property
        value (str,number): the value of the property

        fixed (bool): if true, the value cannot be changed after the property is created
        hidden (bool): if true, the value is hidden on the platform
        category (str): the category of the property, default: "Blackfynn"
        data_type (str): one of 'string', 'integer', 'double', 'date', 'user'

    """
    _data_types = ['string', 'integer', 'double', 'date', 'user', 'boolean']
    def __init__(self, key, value, fixed=False, hidden=False, category="Blackfynn", data_type=None):
        self.key = key
        self.fixed = fixed
        self.hidden = hidden
        self.category = category

        if data_type is None or (data_type.lower() not in self._data_types):
            dt,v = get_data_type(value)
            self.data_type = dt
            self.value = v
        else:
            self.data_type = data_type
            self.value = value_as_type(value, data_type.lower())

    def as_dict(self):
        """
        Representation of instance as dictionary, used when calling API.
        """
        return {
            "key": self.key,
            "value": str(self.value), # value needs to be string :-(
            "dataType": self.data_type,
            "fixed": self.fixed,
            "hidden": self.hidden,
            "category": self.category
        }

    @classmethod
    def from_dict(cls, data, category='Blackfynn'):
        """
        Create an instance from dictionary, used when handling API response.
        """
        return cls(
            key=data['key'],
            value=data['value'],
            category=category,
            fixed=data['fixed'],
            hidden=data['hidden'],
            data_type=data['dataType']
        )

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return u"<Property key='{}' value='{}' type='{}' category='{}'>" \
                    .format(self.key, self.value, self.data_type, self.category)


def _get_all_class_args(cls):
    # possible class arguments
    if cls == object:
        return set()
    class_args = set()
    for base in cls.__bases__:
        # get all base class argument variables
        class_args.update(_get_all_class_args(base))
    # return this class and all base-class variables
    class_args.update(cls.__init__.__func__.func_code.co_varnames)
    return class_args


class BaseNode(object):
    """
    Base class to serve all objects
    """
    _api = None
    _object_key = 'content'

    def __init__(self, id=None, *args, **kargs):
        self.id = id

    @classmethod
    def from_dict(cls, data, api=None, object_key=None):
        # which object_key are we going to use?
        if object_key is not None:
            obj_key = object_key
        else:
            obj_key = cls._object_key

        # validate obj_key
        if obj_key == '' or obj_key is None:
            content = data
        else:
            content = data[obj_key]

        class_args = _get_all_class_args(cls)

        # find overlapping keys
        kwargs = {}
        thing_id = content.pop('id', None)
        for k,v in content.iteritems():
            # check lower case var names
            k_lower = k.lower()
            # check camelCase --> camel_case
            k_camel = re.sub(r'[A-Z]', lambda x: '_'+x.group(0).lower(), k)
            # check s3case --> s3_case
            k_camel_num = re.sub(r'[0-9]', lambda x: x.group(0)+'_', k)

            # match with existing args
            if k_lower in class_args:
                key = k_lower
            elif k_camel in class_args:
                key = k_camel
            elif k_camel_num in class_args:
                key = k_camel_num
            else:
               key = k

            # assign
            kwargs[key] = v

        # init class with args
        item = cls.__new__(cls)
        cls.__init__(item, **kwargs)

        if thing_id is not None:
            item.id = thing_id

        if api is not None:
            item._api = api
            item._api.core.set_local(item)

        return item

    def __eq__(self, item):
        if self.exists and item.exists:
            return self.id == item.id
        else:
            return self is item

    @property
    def exists(self):
        """
        Whether or not the instance of this object exists on the platform.
        """
        return self.id is not None

    def _check_exists(self):
        if not self.exists:
            raise Exception('Object must be created on the platform before method is called.')

    def __str__(self):
        return self.__repr__()


class BaseDataNode(BaseNode):
    """
    Base class to serve all "data" node-types on platform, e.g. Packages and Collections.
    """
    _type_name = 'packageType'

    def __init__(self, name, type,
            parent=None,
            owner_id=None,
            dataset_id=None,
            id=None,
            provenance_id=None, **kwargs):

        super(BaseDataNode, self).__init__(id=id)

        self.name = name
        self._properties = {}
        if isinstance(parent, basestring) or parent is None:
            self.parent = parent
        elif isinstance(parent, Collection):
            self.parent = parent.id
        else:
            raise Exception("Invalid parent {}".format(parent))
        self.type = type
        self.dataset = dataset_id
        self.owner_id = owner_id
        self.provenance_id = provenance_id

        self.state = kwargs.pop('state', None)
        self.created_at = kwargs.pop('createdAt', None)
        self.updated_at = kwargs.pop('updatedAt', None)

    def update_properties(self):
        self._api.data.update_properties(self)

    def _set_properties(self, *entries):
        # Note: Property is stored as dict of key:properties-entry to enable
        #       over-write of properties values based on key
        for entry in entries:
            assert type(entry) is Property, "Properties wrong type"
            if entry.category not in self._properties:
                self._properties[entry.category] = {}
            self._properties[entry.category].update({entry.key:entry})

    def add_properties(self, *entries):
        """
        Add properties to object.

        Args:
            entries (list): list of Property objects to add to this object

        """
        self._set_properties(*entries)

        # update on platform
        if self.exists:
            self.update_properties()

    def insert_property(self, key, value, fixed=False, hidden=False, category="Blackfynn", data_type=None):
        """
        Add property to object using simplified interface.

        Args:
            key (str): the key of the property
            value (str,number): the value of the property

            fixed (bool): if true, the value cannot be changed after the property is created
            hidden (bool): if true, the value is hidden on the platform
            category (str): the category of the property, default: "Blackfynn"
            data_type (str): one of 'string', 'integer', 'double', 'date', 'user'

        Note:
            This method is being depreciated in favor of ``set_property()`` method (see below).

        """
        return self.set_property(
            key=key,
            value=value,
            fixed=fixed,
            hidden=hidden,
            category=category,
            data_type=data_type
        )

    def set_property(self, key, value, fixed=False, hidden=False, category="Blackfynn", data_type=None):
        """
        Add property to object using simplified interface.

        Args:
            key (str): the key of the property
            value (str,number): the value of the property

            fixed (bool): if true, the value cannot be changed after the property is created
            hidden (bool): if true, the value is hidden on the platform
            category (str): the category of the property, default: "Blackfynn"
            data_type (str): one of 'string', 'integer', 'double', 'date', 'user'

        """
        self._set_properties(
            Property(
                key=key,
                value=value,
                fixed=fixed,
                hidden=hidden,
                category=category,
                data_type=data_type)
        )
        # update on platform, if possible
        if self.exists:
            self.update_properties()

    @property
    def properties(self):
        """
        Returns a list of properties attached to object.
        """
        props = []
        for category in self._properties.values():
            props.extend(category.values())
        return props

    def get_property(self, key, category='Blackfynn'):
        """
        Returns a single property for the provided key, if available

        Args:
            key (str): key of the desired property
            category (str, optional): category of property

        Returns:
            object of type ``Property``

        Example::

            pkg.set_property('quality', 85.0)
            pkg.get_property('quality')

        """
        return self._properties[category].get(key, None)

    def remove_property(self, key, category='Blackfynn'):
        """
        Removes property of key ``key`` and category ``category`` from the object.

        Args:
            key (str): key of property to remove
            category (str, optional): category of property to remove

        """
        if key in self._properties[category]:
            # remove by setting blank
            self._properties[category][key].value = ""
            # update remotely
            self.update_properties()
            # get rid of it locally
            self._properties[category].pop(key)

    def update(self, **kwargs):
        """
        Updates object on the platform (with any local changes) and syncs
        local instance with API response object.

        Exmple::

            pkg = bf.get('N:package:1234-1234-1234-1234')
            pkg.name = "New name"
            pkg.update()

        """
        self._check_exists()
        r = self._api.core.update(self, **kwargs)
        _update_self(self, r)

    def delete(self):
        """
        Delete object from platform.
        """
        self._check_exists()
        r = self._api.core.delete(self)
        self.id = None

    def set_ready(self, **kwargs):
        """
        Set's the package's state to ``READY``
        """
        self.state = "READY"
        return self.update(**kwargs)

    def set_unavailable(self):
        """
        Set's the package's state to ``UNAVAILABLE``
        """
        self._check_exists()
        self.state = "UNAVAILABLE"
        return self.update()

    def set_error(self):
        """
        Set's the package's state to ``ERROR``
        """
        self._check_exists()
        self.state = "ERROR"
        return self.update()

    def as_dict(self):
        d = {
            "name": self.name,
            self._type_name: self.type,
            "properties": [
                m.as_dict() for m in self.properties
            ]
        }

        for k in ['parent', 'state', 'dataset']:
            kval = self.__dict__.get(k, None)
            if hasattr(self, k) and kval is not None:
                d[k] = kval

        if self.provenance_id is not None:
            d["provenanceId"] = self.provenance_id

        return d

    @classmethod
    def from_dict(cls, data, *args, **kwargs):
        item = super(BaseDataNode,cls).from_dict(data, *args, **kwargs)

        try:
            item.state = data['content']['state']
        except:
            pass

        # parse, store parent (ID only)
        parent = data.get('parent', None)
        if parent is not None:
            if isinstance(parent, basestring):
                item.parent = parent
            else:
                pkg_cls = get_package_class(parent)
                p = pkg_cls.from_dict(parent, *args, **kwargs)
                item.parent = p.id

        def cls_add_property(prop):
            cat = prop.category
            if cat not in item._properties:
                item._properties[cat] = {}
            item._properties[cat].update({prop.key: prop})

        # parse properties
        if 'properties' in data:
            for entry in data['properties']:
                if 'properties' not in entry:
                    # flat list of properties: [entry]
                    prop = Property.from_dict(entry, category=entry['category'])
                    cls_add_property(prop)
                else:
                    # nested properties list [ {category,entry} ]
                    category = entry['category']
                    for prop_entry in entry['properties']:
                        prop = Property.from_dict(prop_entry, category=category)
                        cls_add_property(prop)

        return item


class BaseCollection(BaseDataNode):
    """
    Base class used for both ``Dataset`` and ``Collection``.
    """
    def __init__(self, name, package_type, **kwargs):
        self.storage = kwargs.pop('storage', None)
        super(BaseCollection, self).__init__(name, package_type, **kwargs)

        # items is None until an API response provides the item objects
        # to be parsed, which then updates this instance.
        self._items = None

    def add(self, *items):
        """
        Add items to the Collection/Dataset.
        """
        self._check_exists()
        for item in items:
            # initialize if need be
            if self._items is None:
                self._items = []
            if isinstance(self, Dataset):
                item.parent = None
                item.dataset = self.id
            elif hasattr(self, 'dataset'):
                item.parent = self.id
                item.dataset = self.dataset

            # create, if not already created
            new_item = self._api.core.create(item)
            item.__dict__.update(new_item.__dict__)

            # add item
            self._items.append(item)

    def remove(self, *items):
        """
        Removes items, where items can be an object or the object's ID (string).
        """
        self._check_exists()
        for item in items:
            if item not in self._items:
                raise Exception('Cannot remove item, not in collection:{}'.format(item))

        self._api.data.delete(*items)
        # force refresh
        self._items = None

    @property
    def items(self):
        """
        Get all items inside Dataset/Collection (i.e. non-nested items).

        Note:
            You can also iterate over items inside a Dataset/Colleciton without using ``.items``::

                for item in my_dataset:
                    print "item name = ", item.name

        """
        self._check_exists()
        if self._items is None:
            new_self = self._get_method(self)
            new_items = new_self._items
            self._items = new_items if new_items is not None else []

        return self._items

    @property
    def _get_method(self):
        pass

    def print_tree(self, indent=0):
        """
        Prints a tree of **all** items inside object.
        """
        self._check_exists()
        print u'{}{}'.format(' '*indent, self)
        for item in self.items:
            if isinstance(item, BaseCollection):
                item.print_tree(indent=indent+2)
            else:
                print u'{}{}'.format(' '*(indent+2), item)

    def get_items_by_name(self, name):
        """
        Get an item inside of object by name (if match is found).

        Args:
            name (str): the name of the item

        Returns:
            list of matches

        Note:
            This only works for **first-level** items, meaning it must exist directly inside the current object;
            nested items will not be returned.

        """
        self._check_exists()
        # note: non-hierarchical
        return filter(lambda x: x.name==name, self.items)

    def get_items_names(self):
        self._check_exists()
        return map(lambda x: x.name, self.items)

    def upload(self, *files, **kwargs):
        """
        Upload files into current object.

        Args:
            files: list of local files to upload.

        Example::

            my_collection.upload('/path/to/file1.nii.gz', '/path/to/file2.pdf')

        """
        self._check_exists()
        return self._api.io.upload_files(self, files, append=False, **kwargs)

    def create_collection(self, name):
        """
        Create a new collection within the current object. Collections can be created within
        datasets and within other collections.

        Args:
            name (str): The name of the to-be-created collection

        Returns:
            The created ``Collection`` object.

        Example::

              from blackfynn import Blackfynn()

              bf = Blackfynn()
              ds = bf.get_dataset('my_dataset')

              # create collection in dataset
              col1 = ds.create_collection('my_collection')

              # create collection in collection
              col2 = col1.create_collection('another_collection')

        """
        c = Collection(name)
        self.add(c)
        return c

    # sequence-like method
    def __getitem__(self, i):
        self._check_exists()
        return self.items[i]

    # sequence-like method
    def __len__(self):
        self._check_exists()
        return len(self.items)

    # sequence-like method
    def __delitem__(self, key):
        self._check_exists()
        self.remove(key)

    def __iter__(self):
        self._check_exists()
        for item in self.items:
            yield item

    # sequence-like method
    def __contains__(self, item):
        """
        Tests if item is in the collection, where item can be either
        an object's ID (string) or an object's instance.
        """
        self._check_exists()
        if isinstance(item, basestring):
            some_id = self._api.data._get_id(item)
            item_ids = [x.id for x in self.items]
            contains = some_id in item_ids
        elif self._items is None:
            return False
        else:
            return item in self._items

        return contains

    def as_dict(self):
        d = super(BaseCollection, self).as_dict()
        if self.owner_id is not None:
            d['owner'] = self.owner_id
        return d

    @classmethod
    def from_dict(cls, data, *args, **kwargs):
        item = super(BaseCollection, cls).from_dict(data, *args, **kwargs)
        children = []
        if 'children' in data:
            for child in data['children']:
                pkg_cls = get_package_class(child)
                kwargs['api'] = item._api
                pkg = pkg_cls.from_dict(child, *args, **kwargs)
                children.append(pkg)
        item.add(*children)

        return item

    def __repr__(self):
        return u"<BaseCollection name='{}' id='{}'>".format(self.name, self.id)


class DataPackage(BaseDataNode):
    """
    DataPackage is the core data object representation on the platform.

    Args:
        name (str):          The name of the data package
        package_type (str):  The package type, e.g. 'TimeSeries', 'MRI', etc.

    Note:
        ``package_type`` must be a supported package type. See our data type
        registry for supported values.

    """

    def __init__(self, name, package_type, **kwargs):
        self.storage = kwargs.pop('storage', None)
        super(DataPackage, self).__init__(name=name, type=package_type, **kwargs)
        # local-only attribute
        self.session = None

    def set_view(self, *files):
        """
        Set the object(s) used to view the package, if not the file(s) or source(s).
        """
        self._check_exists()
        ids = self._api.packages.set_view(self, *files)
        # update IDs of file objects
        for i,f in enumerate(files):
            f.id = ids[i]

    def set_files(self, *files):
        """
        Sets the files of a DataPackage. Files are typically modified
        source files (e.g. converted to a different format).
        """
        self._check_exists()
        ids = self._api.packages.set_files(self, *files)
        # update IDs of file objects
        for i,f in enumerate(files):
            f.id = ids[i]

    def set_sources(self, *files):
        """
        Sets the sources of a DataPackage. Sources are the raw, unmodified
        files (if they exist) that contains the package's data.
        """
        self._check_exists()
        ids = self._api.packages.set_sources(self, *files)
        # update IDs of file objects
        for i,f in enumerate(files):
            f.id = ids[i]

    def append_to_files(self, *files):
        """
        Append to file list of a DataPackage
        """
        self._check_exists()
        files = self._api.packages.set_files(self, *files, append=True)

    def append_to_sources(self, *files):
        """
        Appends to source list of a DataPackage.
        """
        self._check_exists()
        files = self._api.packages.set_sources(self, *files, append=True)

    @property
    def sources(self):
        """
        Returns the sources of a DataPackage. Sources are the raw, unmodified
        files (if they exist) that contains the package's data.
        """
        self._check_exists()
        return self._api.packages.get_sources(self)

    @property
    def files(self):
        """
        Returns the files of a DataPackage. Files are the possibly modified
        source files (e.g. converted to a different format), but they could also
        be the source files themselves.
        """
        self._check_exists()
        return self._api.packages.get_files(self)

    @property
    def view(self):
        """
        Returns the object(s) used to view the package. This is typically a set of
        file objects, that may be the DataPackage's sources or files, but could also be
        a unique object specific for the viewer.
        """
        self._check_exists()
        return self._api.packages.get_view(self)

    def link(self, record, values=None):
        """
        Links a ``DataPackage`` to a ``Record`` given a type of ``belongs_to``.

        Args:
            relationship_type (RelationshipType or str): type of relationship to create
            record (Record): record that is related to the data package
            values (dict, optional): values for properties definied in the relationship's schema

        Returns:
            ``Relationship`` that defines the link

        Example:
            Create a link between a data package and a record::

                eeg.link('from', mouse_001)

            Create a link (with values) between a data package and record::

                eeg.link('from', mouse_001, {"date": datetime.datetime(1991, 02, 26, 07, 0)})

        Note:
            Relationship direction is ``Record`` --to--> ``DataPackage``.
        """
        values = dict() if values is None else values
        self._check_exists()
        assert isinstance(record, Record), "record must be object of type Record"

        # auto-create relationship type
        relationships = self._api.concepts.relationships.get_all(self.dataset)
        if 'belongs_to' not in relationships:
            r = RelationshipType(dataset_id=self.dataset, name='belongs_to', description='belongs_to')
            self._api.concepts.relationships.create(self.dataset, r)

        return self._api.concepts.relationships.instances.link(self.dataset, 'belongs_to', record, self, values)

    def as_dict(self):
        d = super(DataPackage, self).as_dict()
        if self.owner_id is not None:
            d['owner'] = self.owner_id
        return d

    @classmethod
    def from_dict(cls, data, *args, **kwargs):
        item = super(DataPackage, cls).from_dict(data, *args, **kwargs)

        # parse objects
        objects = data.get('objects', None)
        if objects is not None:
            for otype in ['sources','files','view']:
                if otype not in data['objects']:
                    continue
                odata = data['objects'][otype]
                item.__dict__[otype] = [File.from_dict(x) for x in odata]

        return item

    @classmethod
    def from_id(cls, id):
        return self._api.packages.get(id)

    def __repr__(self):
        return u"<DataPackage name='{}' id='{}'>".format(self.name, self.id)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Files
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class File(BaseDataNode):
    """
    File node on the Blackfynn platform. Points to some S3 location.

    Args:
        name (str):      Name of the file (without extension)
        s3_key (str):    S3 key of file
        s3_bucket (str): S3 bucket of file
        file_type (str): Type of file, e.g. 'MPEG', 'PDF'
        size (long): Size of file

    Note:
        ``file_type`` must be a supported file type. See our file type registry
        for a list of supported file types.


    """
    _type_name = 'fileType'

    def __init__(self, name, s3_key, s3_bucket, file_type, size, pkg_id=None, **kwargs):
        super(File, self).__init__(name, type=file_type, **kwargs)

        # data
        self.s3_key = s3_key
        self.s3_bucket = s3_bucket
        self.size = size
        self.pkg_id = pkg_id
        self.local_path = None

    def as_dict(self):
        d = super(File, self).as_dict()
        d.update({
            "s3bucket": self.s3_bucket,
            "s3key": self.s3_key,
            "size": self.size
        })
        d.pop('parent', None)
        props = d.pop('properties')
        return {
            'objectType': 'file',
            'content': d,
            'properties': props
        }

    @property
    def url(self):
        """
        The presigned-URL of the file.
        """
        self._check_exists()
        return self._api.packages.get_presigned_url_for_file(self.pkg_id, self.id)

    def download(self, destination):
        """
        Download the file.

        Args:
            destination (str): path for downloading; can be absolute file path,
                               prefix or destination directory.

        """
        if self.type=="DirectoryViewerData":
            raise NotImplementedError("Downloading S3 directories is currently not supported")

        if os.path.isdir(destination):
            # destination dir
            f_local = os.path.join(destination, os.path.basename(self.s3_key))
        if '.' not in os.path.basename(destination):
            # destination dir + prefix
            f_local = destination + '_' + os.path.basename(self.s3_key)
        else:
            # exact location
            f_local = destination

        r = requests.get(self.url, stream=True)
        with open(f_local, 'wb') as f:
            for chunk in r.iter_content(chunk_size=1024):
                if chunk: f.write(chunk)

        # set local path
        self.local_path = f_local

        return f_local

    def __repr__(self):
        return u"<File name='{}' type='{}' key='{}' bucket='{}' size='{}' id='{}'>" \
                    .format(self.name, self.type, self.s3_key, self.s3_bucket, self.size, self.id)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Time series
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class TimeSeries(DataPackage):
    """
    Represents a timeseries package on the platform. TimeSeries packages
    contain channels, which contain time-dependent data sampled at some
    frequency.

    Args:
        name:  The name of the timeseries package

    """
    def __init__(self, name, **kwargs):
        kwargs.pop('package_type', None)
        super(TimeSeries,self).__init__(name=name, package_type="TimeSeries", **kwargs)


    def streaming_credentials(self):
        self._check_exists()
        return self._api.timeseries.get_streaming_credentials(self)

    @property
    def start(self):
        """
        The start time of time series data (over all channels)
        """
        self._check_exists()
        return sorted([x.start for x in self.channels])[0]

    @property
    def end(self):
        """
        The end time (in usecs) of time series data (over all channels)
        """
        self._check_exists()
        return sorted([x.end for x in self.channels])[-1]

    def limits(self):
        """
        Returns time limit tuple (start, end) of package.
        """
        channels = self.channels
        start = sorted([x.start for x in channels])[0]
        end   = sorted([x.end   for x in channels])[-1]
        return start, end

    def segments(self, start=None, stop=None):
        """
        Returns list of contiguous data segments available for package. Segments are
        assesssed for all channels, and the union of segments is returned.
        
        Args:
            start (int, datetime, optional): return segments starting after this time
                                             (default earliest start of any channel)
            stop (int, datetime, optional):  return segments starting before this time
                                             (default latest end time of any channel)

        Returns:
            List of tuples, where each tuple represents the (start, stop) of contiguous data.
        """
        # flattenened list of segments across all channels
        channel_segments = [
            segment for channel in self.channels 
            for segment in channel.segments(start=start, stop=stop)
        ]
        # union all segments
        union_segments = []
        for begin,end in sorted(channel_segments):
            if union_segments and union_segments[-1][1] >= begin - 1:
                new_segment = (union_segments[-1][0], max(union_segments[-1][1], end))
                union_segments.pop()
                union_segments.append(new_segment)
            else:
                union_segments.append((begin, end))
        return union_segments

    @property
    def channels(self):
        """
        Returns list of Channel objects associated with package.

        Note:
            This is a dynamically generated property, so every call will make an API request.

            Suggested usage::

                channels = ts.channels
                for ch in channels:
                    print ch

            This will be much slower, as the API request is being made each time.::

                for ch in ts.channels:
                    print ch

        """
        self._check_exists()
        # always dynamically return channel list
        return self._api.timeseries.get_channels(self)

    def get_channel(self, channel):
        """
        Get channel by ID.

        Args:
            channel (str): ID of channel
        """
        self._check_exists()
        return self._api.timeseries.get_channel(self, channel)

    def add_channels(self, *channels):
        """
        Add channels to TimeSeries package.

        Args:
            channels: list of Channel objects.

        """
        self._check_exists()
        for channel in channels:
            ch = self._api.timeseries.create_channel(self, channel)
            channel.__dict__.update(ch.__dict__)

    def remove_channels(self, *channels):
        """
        Remove channels from TimeSeries package.

        Args:
            channels: list of Channel objects or IDs
        """
        self._check_exists()
        for channel in channels:
            self._api.timeseries.delete_channel(channel)
            channel.id = None
            channel._pkg = None

    # ~~~~~~~~~~~~~~~~~~
    # Data
    # ~~~~~~~~~~~~~~~~~~
    def get_data(self, start=None, end=None, length=None, channels=None, use_cache=True):
        """
        Get timeseries data between ``start`` and ``end`` or ``start`` and ``start + length``
        on specified channels (default all channels).

        Args:
            start (optional): start time of data (usecs or datetime object)
            end (optional): end time of data (usecs or datetime object)
            length (optional): length of data to retrieve, e.g. '1s', '5s', '10m', '1h'
            channels (optional): list of channel objects or IDs, default all channels.

        Note:
            Data requests will be automatically chunked and combined into a single Pandas
            DataFrame. However, you must be sure you request only a span of data that
            will properly fit in memory.

            See ``get_data_iter`` for an iterator approach to timeseries data retrieval.

        Example:

            Get 5 seconds of data from start over all channels::

                data = ts.get_data(length='5s')

            Get data betwen 12345 and 56789 (representing usecs since Epoch)::

                data = ts.get_data(start=12345, end=56789)

            Get first 10 seconds for the first two channels::

                data = ts.get_data(length='10s', channels=ts.channels[:2])

        """
        self._check_exists()
        return self._api.timeseries.get_ts_data(self,start=start, end=end, length=length, channels=channels, use_cache=use_cache)

    def get_data_iter(self, channels=None, start=None, end=None, length=None, chunk_size=None, use_cache=True):
        """
        Returns iterator over the data. Must specify **either ``end`` OR ``length``**, not both.

        Args:
            channels (optional): channels to retrieve data for (default: all)
            start: start time of data (default: earliest time available).
            end: end time of data (default: latest time avialable).
            length: some time length, e.g. '1s', '5m', '1h' or number of usecs
            chunk: some time length, e.g. '1s', '5m', '1h' or number of usecs

        Returns:
            iterator of Pandas Series, each the size of ``chunk_size``.

        """
        self._check_exists()
        return self._api.timeseries.get_ts_data_iter(self, channels=channels, start=start, end=end, length=length, chunk_size=chunk_size, use_cache = use_cache)

    def write_annotation_file(self,file,layer_names = None):
        """
        Writes all layers to a csv .bfannot file

        Args:
            file : path to .bfannot output file. Appends extension if necessary
            layer_names (optional): List of layer names to write

        """

        return self._api.timeseries.write_annotation_file(self,file,layer_names)

    def append_annotation_file(self,file):
        """
        Processes .bfannot file and adds to timeseries package.

        Args:
            file : path to .bfannot file

        """
        self._check_exists()
        return self._api.timeseries.process_annotation_file(self,file)

    def append_files(self, *files, **kwargs):
        self._check_exists()
        return self._api.io.upload_files(self, files, append=True, **kwargs)

    def stream_data(self, data):
        self._check_exists()
        return self._api.timeseries.stream_data(self, data)

    # ~~~~~~~~~~~~~~~~~~
    # Annotations
    # ~~~~~~~~~~~~~~~~~~

    @property
    def layers(self):
        """
        List of annotation layers attached to TimeSeries package.
        """
        self._check_exists()
        # always dynamically return annotation layers
        return self._api.timeseries.get_annotation_layers(self)

    def get_layer(self, id_or_name):
        """
        Get annotation layer by ID or name.

        Args:
            id_or_name: layer ID or name
        """
        self._check_exists()
        layers = self.layers
        matches = filter(lambda x: x.id==id_or_name, layers)
        if len(matches) == 0:
            matches = filter(lambda x: x.name==id_or_name, layers)

        if len(matches) == 0:
            raise Exception("No layers match criteria.")
        if len(matches) > 1:
            raise Exception("More than one layer matched criteria")

        return matches[0]

    def add_layer(self,layer,description=None):
        """
        Args:
            layer:   TimeSeriesAnnotationLayer object or name of annotation layer
            description (str, optional):   description of layer

        """
        self._check_exists()
        return self._api.timeseries.create_annotation_layer(self,layer=layer,description=description)

    def add_annotations(self,layer,annotations):
        """
        Args:
            layer: either TimeSeriesAnnotationLayer object or name of annotation layer.
                   Note that non existing layers will be created.
            annotations: TimeSeriesAnnotation object(s)

        Returns:
            list of TimeSeriesAnnotation objects
        """
        self._check_exists()
        cur_layer = self._api.timeseries.create_annotation_layer(self,layer=layer,description=None)
        return self._api.timeseries.create_annotations(layer=cur_layer, annotations=annotations)

    def insert_annotation(self,layer,annotation,start=None,end=None,channel_ids=None,annotation_description=None):
        """
        Insert annotations using a more direct interface, without the need for layer/annotation objects.

        Args:
            layer: str of new/existing layer or annotation layer object
            annotation: str of annotation event

            start (optional): start of annotation
            end (optional): end of annotation
            channels_ids (optional): list of channel IDs to apply annotation
            annotation_description (optional): description of annotation

        Example:
            To add annotation on layer "my-events" across all channels::

                ts.insert_annotation('my-events', 'my annotation event')

            To add annotation to first channel::

                ts.insert_annotation('my-events', 'first channel event', channel_ids=ts.channels[0])

        """
        self._check_exists()
        cur_layer = self._api.timeseries.create_annotation_layer(self,layer=layer,description=None)
        return self._api.timeseries.create_annotation(
                layer=cur_layer,
                annotation=annotation,
                start=start,
                end=end,
                channel_ids=channel_ids,
                description=annotation_description)

    def delete_layer(self, layer):
        """
        Delete annotation layer.

        Args:
            layer: annotation layer object

        """
        self._check_exists()
        return self._api.timeseries.delete_annotation_layer(layer)

    def query_annotation_counts(self, channels, start, end, layer=None):
        """
        Get annotation counts between ``start`` and ``end``.

        Args:
            channels: list of channels to query over
            start: start time of query (datetime object or usecs from Epoch)
            end: end time of query (datetime object or usecs from Epoch)

        """
        self._check_exists()
        return self._api.timeseries.query_annotation_counts(
            channels=channels,start=start,end=end,layer=layer)

    def __repr__(self):
        return u"<TimeSeries name=\'{}\' id=\'{}\'>".format(self.name, self.id)


class TimeSeriesChannel(BaseDataNode):
    """
    TimeSeriesChannel represents a single source of time series data. (e.g. electrode)

    Args:
        name (str):                   Name of channel
        rate (float):                 Rate of the channel (Hz)
        start (optional):             Absolute start time of all data (datetime obj)
        end (optional):               Absolute end time of all data (datetime obj)
        unit (str, optional):         Unit of measurement
        channel_type (str, optional): One of 'continuous' or 'event'
        source_type (str, optional):  The source of data, e.g. "EEG"
        group (str, optional):        The channel group, default: "default"

    """
    def __init__(self, name, rate, start=0, end=0, unit='V', channel_type='continuous', source_type='unspecified', group="default", last_annot=0, spike_duration=None, **kwargs):
        self.channel_type = channel_type.upper()

        super(TimeSeriesChannel, self).__init__(name=name, type=self.channel_type,**kwargs)

        self.rate = rate
        self.unit = unit
        self.last_annot = last_annot
        self.group = group
        self.start = start
        self.end = end
        self.spike_duration = spike_duration

        self.set_property("Source Type", source_type.upper(), fixed=True, hidden=True, category="Blackfynn")

        ###  local-only
        # parent package
        self._pkg = None
        # sample period (in usecs)
        self._sample_period = 1.0e6/self.rate

    @property
    def start(self):
        """
        The start time of channel data (microseconds since Epoch)
        """
        return self._start

    @start.setter
    def start(self, start):
        self._start = infer_epoch(start)

    @property
    def start_datetime(self):
        return usecs_to_datetime(self._start)

    @property
    def end(self):
        """
        The end time (in usecs) of channel data (microseconds since Epoch)
        """
        return self._end

    @end.setter
    def end(self, end):
        self._end = infer_epoch(end)

    @property
    def end_datetime(self):
        return usecs_to_datetime(self._end)

    def _page_delta(self, page_size):
        return long((1.0e6/self.rate) * page_size)

    def update(self):
        self._check_exists()
        r = self._api.timeseries.update_channel(self)
        self.__dict__.update(r.__dict__)

    def segments(self, start=None, stop=None):
        """
        Return list of contiguous segments of valid data for channel.

        Args:
            start (long, datetime, optional): return segments starting after this time (default start of channel)
            stop (long, datetime, optional):  return segments starting before this time (default end of channel)

        Returns:
            List of tuples, where each tuple represents the (start, stop) of contiguous data.
        """
        start = self.start if start is None else start
        stop  = self.end   if stop  is None else stop
        return self._api.timeseries.get_segments(self._pkg, self, start=start, stop=stop)

    @property
    def gaps(self):
        # TODO: infer gaps from segments
        raise NotImplementedError

    def update_properties(self):
        self._api.timeseries.update_channel_properties(self)

    def get_data(self, start=None, end=None, length=None, use_cache=True):
        """
        Get channel data between ``start`` and ``end`` or ``start`` and ``start + length``

        Args:
            start     (optional): start time of data (usecs or datetime object)
            end       (optional): end time of data (usecs or datetime object)
            length    (optional): length of data to retrieve, e.g. '1s', '5s', '10m', '1h'
            use_cache (optional): whether to use locally cached data

        Returns:
            Pandas Series containing requested data for channel.

        Note:
            Data requests will be automatically chunked and combined into a single Pandas
            Series. However, you must be sure you request only a span of data that
            will properly fit in memory.

            See ``get_data_iter`` for an iterator approach to timeseries data retrieval.

        Example:

            Get 5 seconds of data from start over all channels::

                data = channel.get_data(length='5s')

            Get data betwen 12345 and 56789 (representing usecs since Epoch)::

                data = channel.get_data(start=12345, end=56789)
        """

        return self._api.timeseries.get_ts_data(
                ts         = self._pkg,
                start      = start,
                end        = end,
                length     = length,
                channels   = [self],
                use_cache  = use_cache)

    def get_data_iter(self, start=None, end=None, length=None, chunk_size=None, use_cache=True):
        """
        Returns iterator over the data. Must specify **either ``end`` OR ``length``**, not both.

        Args:
            start      (optional): start time of data (default: earliest time available).
            end        (optional): end time of data (default: latest time avialable).
            length     (optional): some time length, e.g. '1s', '5m', '1h' or number of usecs
            chunk_size (optional): some time length, e.g. '1s', '5m', '1h' or number of usecs
            use_cache  (optional): whether to use locally cached data

        Returns:
            Iterator of Pandas Series, each the size of ``chunk_size``.
        """

        return self._api.timeseries.get_ts_data_iter(
                ts         = self._pkg,
                start      = start,
                end        = end,
                length     = length,
                channels   = [self],
                chunk_size = chunk_size,
                use_cache  = use_cache)

    def as_dict(self):
        return {
            "name": self.name,
            "start": self.start,
            "end": self.end,
            "unit": self.unit,
            "rate": self.rate,
            "channelType": self.channel_type,
            "lastAnnotation": self.last_annot,
            "group": self.group,
            "spikeDuration": self.spike_duration,
            "properties": [x.as_dict() for x in self.properties]
        }

    def __repr__(self):
        return u"<TimeSeriesChannel name=\'{}\' id=\'{}\'>".format(self.name, self.id)


class TimeSeriesAnnotationLayer(BaseNode):
    """
    Annotation layer containing one or more annotations. Layers are used
    to separate annotations into logically distinct groups when applied
    to the same data package.

    Args:
        name:           Name of the layer
        time_series_id: The TimeSeries ID which the layer applies
        description:    Description of the layer

    """
    _object_key = None

    def __init__(self, name, time_series_id, description=None, **kwargs):
        super(TimeSeriesAnnotationLayer,self).__init__(**kwargs)
        self.name = name
        self.time_series_id= time_series_id
        self.description = description

    def iter_annotations(self, window_size=10, channels=None):
        """
        Iterate over annotations according to some window size (seconds).

        Args:
            window_size (float): Number of seconds in window
            channels:            List of channel objects or IDs

        Yields:
            List of annotations found in current window.
        """
        self._check_exists()
        ts = self._api.core.get(self.time_series_id)
        return self._api.timeseries.iter_annotations(
            ts=ts, layer=self, channels=channels, window_size=window_size)

    def add_annotations(self, annotations):
        """
        Add annotations to layer.

        Args:
            annotations (str): List of annotation objects to add.

        """
        self._check_exists()
        return self._api.timeseries.create_annotations(layer=self, annotations=annotations)

    def insert_annotation(self,annotation,start=None,end=None,channel_ids=None,description=None):
        """
        Add annotations; proxy for ``add_annotations``.

        Args:
            annotation (str): Annotation string
            start:            Start time (usecs or datetime)
            end:              End time (usecs or datetime)
            channel_ids:      list of channel IDs

        Returns:
            The created annotation object.
        """
        self._check_exists()
        return self._api.timeseries.create_annotation(layer=self, annotation=annotation,start=start,end=end,channel_ids=channel_ids,description=description)

    def annotations(self, start=None, end=None, channels=None):
        """
        Get annotations between ``start`` and ``end`` over ``channels`` (all channels by default).


        Args:
            start:    Start time
            end:      End time
            channels: List of channel objects or IDs

        """
        self._check_exists()
        ts = self._api.core.get(self.time_series_id)
        return self._api.timeseries.query_annotations(
            ts=ts, layer=self, channels=channels, start=start, end=end)

    def annotation_counts(self, start, end, channels=None):
        """
        The number of annotations between ``start`` and ``end`` over selected
        channels (all by default).

        Args:
            start:    Start time
            end:      End time
            channels: List of channel objects or IDs
        """
        self._check_exists()
        ts = self._api.core.get(self.time_series_id)
        return self._api.timeseries.query_annotation_counts(
            ts=ts, layer=self, channels=channels, start=start, end=end)

    def delete(self):
        """
        Delete annotation layer.
        """
        self._check_exists()
        return self._api.timeseries.delete_annotation_layer(self)

    def as_dict(self):
        return {
            "name" : self.name,
            "description" : self.description
        }

    def __repr__(self):
        return u"<TimeSeriesAnnotationLayer name=\'{}\' id=\'{}\'>".format(self.name, self.id)


class TimeSeriesAnnotation(BaseNode):
    """
    Annotation is an event on one or more channels in a dataset

    Args:
        label (str):    The label for the annotation
        channel_ids:    List of channel IDs that annotation applies
        start:          Start time
        end:            End time
        name:           Name of annotation
        layer_id:       Layer ID for annoation (all annotations exist on a layer)
        time_series_id: TimeSeries package ID
        description:    Description of annotation

    """
    _object_key = None


    def __init__(self, label, channel_ids, start, end, name='',layer_id= None,
                 time_series_id = None, description=None, **kwargs):
        self.user_id = kwargs.pop('userId', None)
        super(TimeSeriesAnnotation,self).__init__(**kwargs)
        self.name = ''
        self.label = label
        self.channel_ids = channel_ids
        self.start = start
        self.end = end
        self.description = description
        self.layer_id = layer_id
        self.time_series_id = time_series_id

    def delete(self):
        self._check_exists()
        return self._api.timeseries.delete_annotation(annot=self)

    def as_dict(self):
        channel_ids = self.channel_ids
        if type(channel_ids) is not list:
            channel_ids = [channel_ids]
        return {
            "name" : self.name,
            "label" : self.label,
            "channelIds": channel_ids,
            "start" : self.start,
            "end" : self.end,
            "description" : self.description,
            "layer_id" : self.layer_id,
            "time_series_id" : self.time_series_id,
        }

    def __repr__(self):
        date = datetime.datetime.fromtimestamp(self.start/1e6)
        return u"<TimeSeriesAnnotation label=\'{}\' layer=\'{}\' start=\'{}\'>".format(self.label, self.layer_id, date.isoformat())


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Tabular
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Tabular(DataPackage):
    """
    Represents a Tabular package on the platform.

    Args:
        name: The name of the package
    """
    def __init__(self, name, **kwargs):
        kwargs.pop('package_type',None)
        super(Tabular,self).__init__(
            name=name,
            package_type="Tabular",
            **kwargs)
        self.schema = None

    def get_data(self,limit=1000, offset=0,order_by = None, order_direction='ASC'):
        """
        Get data from tabular package as DataFrame

        Args:
            limit:           Max number of rows to return (1000 default)
            offset:          Offset when retrieving rows
            order_by:        Column to order data
            order_direction: Ascending ('ASC') or descending ('DESC')

        Returns:
            Pandas DataFrame

        """
        self._check_exists()
        return self._api.tabular.get_tabular_data(self,limit=limit,offset=offset ,order_by=order_by, order_direction=order_direction)

    def get_data_iter(self, chunk_size=10000, offset=0, order_by = None, order_direction='ASC'):
        """
        Iterate over tabular data, each data chunk will be of size ``chunk_size``.
        """
        self._check_exists()
        return self._api.tabular.get_tabular_data_iter(self,chunk_size=chunk_size,offset=offset,order_by=order_by, order_direction=order_direction)

    def set_schema(self, schema):
        self.schema = schema
        # TODO: parse response
        return self._api.tabular.set_table_schema(self, schema)

    def get_schema(self):
        self._check_exists()
        # TODO: parse response
        return self._api.tabular.get_table_schema(self)

    def __repr__(self):
        return u"<Tabular name=\'{}\' id=\'{}\'>".format(self.name, self.id)


class TabularSchema(BaseNode):
    def __init__(self, name, column_schema = [], **kwargs):
        super(TabularSchema, self).__init__(**kwargs)
        self.name = name
        self.column_schema = column_schema

    @classmethod
    def from_dict(cls, data):
        column_schema = []
        for x in data['columns']:
            if 'displayName' not in x.keys():
                x['displayName'] = ''
            column_schema.append(TabularSchemaColumn.from_dict(x))

        return cls(
            name = data['name'],
            id = data['id'],
            column_schema = column_schema
           )

    def as_dict(self):
        column_schema = [dict(
            name = x.name,
            displayName = x.display_name,
            datatype = x.datatype,
            primaryKey = x.primary_key,
            internal = x.internal
        ) for x in self.column_schema]
        return column_schema

    def __repr__(self):
        return u"<TabularSchema name=\'{}\' id=\'{}\'>".format(self.name, self.id)

class TabularSchemaColumn():

    def __init__(self, name, display_name, datatype, primary_key = False, internal = False, **kwargs):
        self.name=name
        self.display_name = display_name
        self.datatype = datatype
        self.internal = internal
        self.primary_key = primary_key

    @classmethod
    def from_dict(cls, data):
        return cls(
            name = data['name'],
            display_name = data['displayName'],
            datatype = data['datatype'],
            primary_key = data['primaryKey'],
            internal = data['internal']
        )

    def __repr__(self):
        return u"<TabularSchemaColumn name='{}' display='{}' is-primary='{}'>".format(self.name, self.display_name, self.primary_key)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# User
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class User(BaseNode):

    _object_key = ''

    def __init__(self,
            email,
            first_name,
            last_name,
            credential='',
            photo_url='',
            url='',
            authy_id=0,
            accepted_terms='',
            color=None,
            is_super_admin=False,
            *args,
            **kwargs):
        kwargs.pop('preferredOrganization', None)
        self.storage = kwargs.pop('storage', None)
        super(User, self).__init__(*args, **kwargs)

        self.email = email
        self.first_name = first_name
        self.last_name = last_name
        self.credential = credential
        self.photo_url = photo_url
        self.color = color
        self.url = url
        self.authy_id = authy_id
        self.accepted_terms = ''
        self.is_super_admin = is_super_admin

    def __repr__(self):
        return u"<User email=\'{}\' id=\'{}\'>".format(self.email, self.id)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Organizations
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Organization(BaseNode):
    _object_key = 'organization'

    def __init__(self,
            name,
            encryption_key_id="",
            slug=None,
            terms=None,
            features=None,
            subscription_state=None,
            *args, **kwargs):
        self.storage = kwargs.pop('storage', None)
        super(Organization, self).__init__(*args, **kwargs)

        self.name = name
        self.terms = terms
        self.features = features or []
        self.subscription_state = subscription_state
        self.encryption_key_id = encryption_key_id
        self.slug = name.lower().replace(' ','-') if slug is None else slug

    @property
    def datasets(self):
        """
        Return all datasets for user for an organization (current context).
        """
        self._check_exists()
        return self._api.datasets.get_all()

    @property
    def members(self):
        return self._api.organizations.get_members(self)

    def __repr__(self):
        return u"<Organization name=\'{}\' id=\'{}\'>".format(self.name, self.id)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Datasets
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Dataset(BaseCollection):
    def __init__(self, name, description=None, **kwargs):
        kwargs.pop('package_type', None)
        kwargs.pop('type', None)
        super(Dataset, self).__init__(name, "DataSet", **kwargs)
        self.description = description or ''

        # remove things that do not apply (a bit hacky)
        for k in ("parent", "type", "set_ready", "set_unavailable", "set_error", "state", "dataset"):
            self.__dict__.pop(k, None)

    def __repr__(self):
        return u"<Dataset name='{}' id='{}'>".format(self.name, self.id)

    @property
    def collaborators(self):
        """
        List of collaborators on Dataset.
        """
        self._check_exists()
        return self._api.datasets.get_collaborators(self)

    def add_collaborators(self, *collaborator_ids):
        """
        Add new collaborator(s) to Dataset.

        Args:
            collaborator_ids: List of collaborator IDs to add (Users, Organizations, Teams)
        """
        self._check_exists()
        return self._api.datasets.add_collaborators(self, *collaborator_ids)

    def remove_collaborators(self, *collaborator_ids):
        """
        Remove collaborator(s) from Dataset.

        Args:
            collaborator_ids: List of collaborator IDs to remove (Users)
        """
        self._check_exists()
        return self._api.datasets.remove_collaborators(self, *collaborator_ids)

    def models(self):
        """
        Returns:
            List of models defined in Dataset
        """
        return self._api.concepts.get_all(self.id)

    def relationships(self):
        """
        Returns:
            List of relationships defined in Dataset
        """
        return self._api.concepts.relationships.get_all(self.id)

    def get_model(self, name_or_id):
        """
        Retrieve a ``Model`` by name or id

        Args:
            name_or_id (str or int): name or id of the model

        Returns:
            The requested ``Model`` in Dataset

        Example::

            mouse = ds.get_model('mouse')
        """
        return self._api.concepts.get(self.id, name_or_id)

    def get_relationship(self, name_or_id):
        """
        Retrieve a ``RelationshipType`` by name or id

        Args:
            name_or_id (str or int): name or id of the relationship

        Returns:
            The requested ``RelationshipType``

        Example::

            belongsTo = ds.get_relationship('belongs-to')
        """
        return self._api.concepts.relationships.get(self.id, name_or_id)

    def create_model(self, name, display_name=None, description=None, schema=None, **kwargs):
        """
        Defines a ``Model`` on the platform.

        Args:
            name (str):                  name of the model
            description (str, optional): description of the model
            schema (dict, optional):     definitation of the model's schema

        Returns:
            The newly created ``Model``

        Example::

            ds.create_concept('mouse', 'Mouse', 'epileptic mice', schema={'id': str, 'weight': float})
        """
        c = Model(dataset_id=self.id, name=name, display_name=display_name, description=description, schema=schema, **kwargs)
        return self._api.concepts.create(self.id, c)

    def create_relationship(self, name, description, schema=None, **kwargs):
        """
        Defines a ``RelationshipType`` on the platform.

        Args:
            name (str):              name of the relationship
            description (str):       description of the relationship
            schema (dict, optional): definitation of the relationship's schema

        Returns:
            The newly created ``RelationshipType``

        Example::

            ds.create_relationship('belongs-to', 'this belongs to that')
        """
        r = RelationshipType(dataset_id=self.id, name=name, description=description, schema=schema, **kwargs)
        return self._api.concepts.relationships.create(self.id, r)

    @property
    def _get_method(self):
        return self._api.datasets.get

    def as_dict(self):
        return dict(
            name = self.name,
            description = self.description,
            properties = [p.as_dict() for p in self.properties]
        )

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Collections
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class Collection(BaseCollection):
    def __init__(self, name, **kwargs):
        kwargs.pop('package_type', None)
        super(Collection, self).__init__(name, package_type="Collection", **kwargs)

    @property
    def _get_method(self):
        return self._api.packages.get

    def __repr__(self):
        return u"<Collection name='{}' id='{}'>".format(self.name, self.id)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Data Ledger
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class LedgerEntry(BaseNode):
    def __init__(self,
            reference,
            userId,
            organizationId,
            metric,
            value,
            date):

        super(LedgerEntry, self).__init__()
        self.reference = reference
        self.userId = userId
        self.organizationId = organizationId
        self.metric = metric
        self.value = value
        self.date = date

    @classmethod
    def from_dict(self, data):
        return LedgerEntry(data["reference"],
                data["userId"],
                data["organizationId"],
                data["metric"],
                data["value"],
                dateutil.parser.parse(data["date"]))

    def as_dict(self):
        return {
                "reference": self.reference,
                "userId": self.userId,
                "organizationId": self.organizationId,
                "metric": self.metric,
                "value": self.value,
                "date": self.date.replace(microsecond=0).isoformat() + 'Z'
                }

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Models & Relationships
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Model Helpers
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

model_type_map = {
    basestring: 'string',
    unicode: 'string',
    str: 'string',
    int: 'long',
    long: 'long',
    float: 'double',
    bool: 'boolean',
    datetime.date: 'date',
    datetime.datetime: 'date'
}

model_type_reverse_map = {
    'string': unicode,
    'long': long,
    'double': float,
    'boolean': bool,
    'date': datetime.datetime
}

def parse_model_datatype(data_type=None):
    if data_type is None or isinstance(data_type, type) and data_type in model_type_map:
        t = data_type
    elif isinstance(data_type, basestring) and data_type.lower() in model_type_reverse_map:
        t = model_type_reverse_map[data_type.lower()]
    else:
        raise Exception('data_type {} not supported'.format(data_type))

    return t

def convert_datatype_to_model_type(data_type):
    if data_type is None:
        return

    assert isinstance(data_type, type) and data_type in model_type_map, "data_type must be one of {}".format(model_type_map.keys())

    t = model_type_map[data_type]
    return t.title()

def cast_value(value, data_type=None):
    if data_type is None or value is None:
        return value

    assert isinstance(data_type, type) and data_type in model_type_map, "data_type must be None or one of {}".format(model_type_map.keys())

    if data_type in (datetime.date, datetime.datetime):
        if isinstance(value, (datetime.date, datetime.datetime)):
            v = value
        else:
            v = dateutil.parser.parse(value)
    else:
        v = data_type(value)

    return v

def uncast_value(value):
    if value is None:
        return

    assert type(value) in model_type_map, "value's type must be one of {}".format(model_type_map.keys())

    if type(value) in (datetime.date, datetime.datetime):
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            value = pytz.utc.localize(value)

        v = value.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + value.strftime('%z')
    else:
        v = value

    return v


class BaseModelProperty(object):
    def __init__(self, name, display_name=None, data_type=basestring, id=None, locked=False, default=True, title=False, description=""):
        assert ' ' not in name, "name cannot contain spaces, alternative names include {} and {}".format(name.replace(" ", "_"), name.replace(" ", "-"))

        self.id = id
        self.name = name
        self.display_name = display_name or name
        self.type = parse_model_datatype(data_type)
        self.locked = locked
        self.default = default
        self.title = title
        self.description = description

    @classmethod
    def from_tuple(cls, data):
        name = data[0]
        data_type = data[1]

        try:
            display_name = data[2]
        except:
            display_name = name

        try:
            title = data[3]
        except:
            title = False

        return cls(name=name, display_name=display_name, data_type=data_type, title=title)

    @classmethod
    def from_dict(cls, data):
        display_name = data.get('displayName', dict())
        data_type = data.get('data_type', data.get('dataType'))
        locked = data.get('locked', False)
        default = data.get('default', True)
        title = data.get('title', data.get('conceptTitle', False))
        id = data.get('id', None)

        return cls(name=data['name'], display_name=display_name, data_type=data_type, id=id, locked=locked, default=default, title=title)

    def as_dict(self):
        return dict(
            id           = self.id,
            name         = self.name,
            displayName  = self.display_name,
            dataType     = convert_datatype_to_model_type(self.type),
            locked       = self.locked,
            default      = self.default,
            conceptTitle = self.title,
            description  = self.description
        )

    def as_tuple(self):
        return (self.name, self.type, self.display_name, self.title)

    def __repr__(self):
        return u"<BaseModelProperty name='{}' {}>".format(self.name, self.type)

class BaseModelValue(object):
    def __init__(self, name, value, *args, **kwargs):
        assert " " not in name, "name cannot contain spaces, alternative names include {} and {}".format(name.replace(" ", "_"), name.replace(" ", "-"))

        self.name = name

        data_type = kwargs.pop('data_type', None)
        self.type = parse_model_datatype(data_type)

        self.set_value(value)

    def set_value(self, value):
        self.value = cast_value(value, self.type)

    @classmethod
    def from_tuple(cls, data):
        return cls(name=data[0], value=value[1])

    @classmethod
    def from_dict(cls, data):
        data_type = data.get('data_type', data.get('dataType'))

        return cls(name=data['name'], value=data['value'], data_type=data_type)

    def as_dict(self):
        return dict(name=self.name, value=uncast_value(self.value), dataType=convert_datatype_to_model_type(self.type))

    def as_tuple(self):
        return (self.name, self.value)

    def __repr__(self):
        return u"<BaseModelValue name='{}' value='{}' {}>".format(self.name, self.value, self.type)


class BaseModelNode(BaseNode):
    _object_key = ''
    _property_cls = BaseModelProperty

    def __init__(self, dataset_id, name, display_name = None, description = None, locked = False, default = True, *args, **kwargs):
        assert " " not in name, "type cannot contain spaces, alternative types include {} and {}".format(name.replace(" ", "_"), name.replace(" ", "-"))

        self.type         = name
        self.dataset_id   = dataset_id
        self.display_name = display_name or name
        self.description  = description or ''
        self.locked       = locked
        self.created_at   = kwargs.pop('createdAt', None)
        self.updated_at   = kwargs.pop('updatedAt', None)
        schema            = kwargs.pop('schema', None)

        super(BaseModelNode, self).__init__(*args, **kwargs)

        self.schema = dict()
        if schema is None:
            return

        self._add_properties(schema)

    def _add_property(self, name, display_name=None, data_type=basestring, title=False):
        prop = self._property_cls(name=name, display_name=display_name, data_type=data_type, title=title)
        self.schema[prop.name] = prop

    def _add_properties(self, properties):
        if isinstance(properties, list):
            for p in properties:
                if isinstance(p, dict):
                    prop = self._property_cls.from_dict(p)
                elif isinstance(p, tuple):
                    prop = self._property_cls.from_tuple(p)
                elif isinstance(p, basestring):
                    prop = self._property_cls(name=p)
                elif isinstance(p, self._property_cls):
                    prop = p
                else:
                    raise Exception("unsupported property value: {}".format(type(p)))

                self.schema[prop.name] = prop
        elif isinstance(properties, dict):
            for k,v in properties.items():
                self._add_property(name=k, data_type=v)
        else:
            raise Exception("invalid type {}; properties must either be a dict or list".format(type(properties)))

    def _validate_values_against_schema(self, values):
        data_keys = set(values.keys())
        schema_keys = set(self.schema.keys())

        assert data_keys <= schema_keys, "Invalid properties: {}.\n\nAn instance of {} should only include values for properties defined in its schema: {}".format(data_keys - schema_keys, self.type, schema_keys)

    # should be overridden by sub-class
    def update(self):
        pass

    def add_property(self, name, data_type=basestring, display_name=None, title=False):
        """
        Appends a property to the object's schema and updates the object on the platform.

        Args:
          name (str): Name of the property
          data_type (type, optional): Python type of the property. Defaults to ``basestring``.
          display_name (str, optional): Display name for the property.

        Example:
          Adding a new property with the default data_type::
            mouse.add_property('name')

          Adding a new property with the ``float`` data_type::
            mouse.add_property('weight', float)
        """
        self._add_property(name, data_type=data_type, display_name=display_name, title=title)

        try:
            self.update()
        except:
            raise #Exception("local object updated, but failed to update remotely")

    def add_properties(self, properties):
        """
        Appends multiple properties to the object's schema and updates the object
        on the platform. Individual properties in the list can be specified in
        multiple ways; as ``tuples``, ``dicts``, or just as the name of the
        property for any properties that are simply strings.

        Args:
          properties (list): List of properties to add

        Example::

            mouse.add_properties([('weight', float), {'name': 'id', 'data_type': long}, 'description'])
        """
        self._add_properties(properties)

        try:
            self.update()
        except:
            raise Exception("local object updated, but failed to update remotely")

    def get_property(self, name):
        """
        Gets the property object by name.

        Example:
            >>> mouse.get_propery('weight').type
            float
        """
        return self.schema.get(name, None)

    def as_dict(self):
        return dict(
            name = self.type,
            displayName = self.display_name,
            description = self.description,
            locked = self.locked,
            schema = [p.as_dict() for p in self.schema.values()]
        )

class BaseRecord(BaseNode):
    _object_key = ''
    _value_cls = BaseModelValue

    def __init__(self, dataset_id, type, *args, **kwargs):
        self.type       = type
        self.dataset_id  = dataset_id
        self.created_at = kwargs.pop('createdAt', None)
        self.updated_at = kwargs.pop('updatedAt', None)
        values          = kwargs.pop('values', None)

        super(BaseRecord, self).__init__(*args, **kwargs)

        self._values = dict()
        if values is None:
            return

        self._set_values(values)

    def _set_value(self, name, value):
        if name in self._values:
            v = self._values[name]
            v.set_value(value)
        else:
            v = self._value_cls(name=name, value=value)
            self._values[v.name] = v

    def _set_values(self, values):
        if isinstance(values, list):
            for v in values:
                if isinstance(v, dict):
                    value = self._value_cls.from_dict(v)
                elif isinstance(v, tuple):
                    value = self._value_cls.from_tuple(v)
                elif isinstance(v, self._value_cls):
                    value = v
                else:
                    raise Exception("unsupported value: {}".format(type(v)))

                self._values[value.name] = value
        elif isinstance(values, dict):
            for k,v in values.items():
                self._set_value(name=k, value=v)
        else:
            raise Exception("invalid type {}; values must either be a dict or list".format(type(properties)))

    @property
    def values(self):
        return { v.name: v.value for v in self._values.values() }

    # should be overridden by sub-class
    def update(self):
        pass

    def get(self, name):
        """
        Returns:
            The value of the property if it exists. None otherwise.
        """
        value = self._values.get(name, None)
        return value.value if value is not None else None

    def set(self, name, value):
        """
        Updates the value of an existing property or creates a new property
        if one with the given name does not exist.

        Note:
            Updates the object on the platform.
        """
        self._set_value(name, value)

        try:
            self.update()
        except:
            raise Exception("local object updated, but failed to update remotely")

    def as_dict(self):
        return {'values': [v.as_dict() for v in self._values.values()] }

    def __repr__(self):
        return u"<BaseRecord type='{}' id='{}'>".format(self.type, self.id)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Models
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class ModelProperty(BaseModelProperty):
    def __repr__(self):
        return u"<ModelProperty name='{}' {}>".format(self.name, self.type)

class ModelValue(BaseModelValue):
    def __repr__(self):
        return u"<ModelValue name='{}' value='{}' {}>".format(self.name, self.value, self.type)


class Model(BaseModelNode):
    """
    Representation of a Model in the knowledge graph.
    """

    _object_key = ''
    _property_cls = ModelProperty

    def __init__(self, dataset_id, name, display_name = None, description = None, locked = False, *args, **kwargs):
        self.count = kwargs.pop('count', None)
        self.state = kwargs.pop('state', None)

        super(Model, self).__init__(dataset_id, name, display_name, description, locked, *args, **kwargs)

    def update(self):
        """
        Updates the details of the ``Model`` on the platform.

        Example::

          mouse.update()

        Note:
            Currently, you can only append new properties to a ``Model``.
        """
        self._check_exists()

        _update_self(self, self._api.concepts.update(self.dataset_id, self))

    def delete(self):
        """
        Deletes a model from the platform. Must not have any instances.
        """
        return self._api.concepts.delete(self.dataset_id, self)

    def get_all(self, limit=100):
        """
        Retrieves all records of the model from the platform.

        Returns:
            List of ``Record``

        Example::

          mice = mouse.get_all()
        """
        return self._api.concepts.instances.get_all(self.dataset_id, self, limit=limit)

    def get(self, id):
        """
        Retrieves a record of the model by id from the platform.

        Args:
            id (int): the id of the record 

        Returns:
            A single ``Record``

        Example::

          mouse_001 = mouse.get(123456789)
        """
        return self._api.concepts.instances.get(self.dataset_id, id, self)

    def create_record(self, values=dict()):
        """
        Creates a record of the model on the platform.

        Args:
            values (dict, optional): values for properties defined in the `Model` schema

        Returns:
            The newly created ``Record``

        Example::

          mouse_002 = mouse.create({"id": 2, "weight": 2.2})

        """
        self._check_exists()

        data_keys = set(values.keys())
        schema_keys = set(self.schema.keys())
        assert len(data_keys & schema_keys) > 0, "An instance of {} must include values for at least one of its propertes: {}".format(self.type, schema_keys)

        self._validate_values_against_schema(values)

        values = [dict(name=k, value=v, dataType=self.schema.get(k).type) for k,v in values.items()]
        ci = Record(dataset_id=self.dataset_id, type=self.type, values=values)
        ci = self._api.concepts.instances.create(self.dataset_id, ci)
        return ci

    def create_records(self, *values_list):
        """
        Creates multiple records of the model on the platform.

        Args:
            values_list (list): array of dictionaries corresponding to record values.

        Returns:
            Array of newly created ``Record``s.
        """
        self._check_exists()
        schema_keys = set(self.schema.keys())

        for values in values_list:
            data_keys = set(values.keys())
            assert len(data_keys & schema_keys) > 0, "An instance of {} must include values for at least one of its propertes: {}".format(self.type, schema_keys)
            self._validate_values_against_schema(values)

        ci_list = [
            Record(
                dataset_id = self.dataset_id,
                type       = self.type,
                values     = [
                    dict(
                        name=k,
                        value=v,
                        dataType=self.schema.get(k).type
                    )
                    for k,v in values.items()
                ]
            )
            for values in values_list
        ]
        return self._api.concepts.instances.create_many(self.dataset_id, self, *ci_list)

    def from_dataframe(self, df):
        return self.create_many(*df.to_dict(orient='records'))

    def delete_records(self, *records):
        """
        Deletes multiple records of a concept from the platform.

        Args:
            *records: instances and/or ids of records to delete

        Returns:
            ``None``

            Prints the list of records that failed to delete.

        Example::

          mouse.delete(mouse_002, 123456789, mouse_003.id)

        """
        result = self._api.concepts.delete_instances(self.dataset_id, self, *instances)

        for error in result['errors']:
            print "Failed to delete instance {} with error: {}".format(error[0], error[1])

    def __iter__(self):
        for record in self.get_all():
            yield record

    def __repr__(self):
        return u"<Model type='{}' id='{}'>".format(self.type, self.id)


class Record(BaseRecord):
    """
    Represents a record of a ``Model``.

    Includes its neighbors, relationships, and links.
    """
    _object_key = ''
    _value_cls = ModelValue

    def __init__(self, dataset_id, type, *args, **kwargs):
        super(Record, self).__init__(dataset_id, type, *args, **kwargs)

    def _get_relationship_type(self, relationship):
        return relationship.type if isinstance(relationship, RelationshipType) else relationship

    def _get_links(self, model):
        return self._api.concepts.instances.relations(self.dataset_id, self, model)

    def links(self, model, relationship=None):
        """
        Returns all neighboring records of the given model type and their relationships to this instance.
        Optionally, filtered by a type of relationship.

        Args:
            model (str, Model):                         type of neighboring model desired
            relationship (str, RelationshipType, optional): relationship type to filter results by

        Returns:
            List of tuples of (``Relationship``, ``Record``)

        Example::
            links = mouse_001.links('disease', 'has')
        """
        links = self._get_links(model)

        if relationship is None:
            return links
        else:
            relationship_type = self._get_relationship_type(relationship)
            filtered = filter(lambda l: l[0].type == relationship_type, links)
            return filtered

    def relationships(self, model, relationship=None):
        """
        All relationships to records of the given model that the current
        record is a member of (as source or destination).

        Args:
            model (str, Model):                         type of neighboring records to find
            relationship (str, RelationshipType, optional): single relationship type to filter results by
        Returns:
            List of ``Relationship``

        Example::
            relationships = mouse_001.neighbors('disease', 'has')

        """
        links = self.links(model, relationship)
        if not links:
            return list()
        else:
            relationships, _ = zip(*links)
            return list(relationships)

    def neighbors(self, model, relationship=None):
        """
        All records of the given model that are related to this record.
        Optionally, filtered by a type of relationship.

        Args:
            model (str, Model):                         type of neighboring records desired
            relationship (str, RelationshipType, optional): relationship type to filter results by

        Returns:
            List of ``Record``

        Example::
            diseases = mouse_001.neighbors('disease', 'has')

        """
        links = self.links(model, relationship)
        if not links:
            return list()
        else:
            _, neighbors = zip(*links)
            return list(neighbors)

    def files(self):
        """
        All files related to the current record.

        Returns:
            List of data objects i.e. ``DataPackage``

        Example::
            eegs = mouse_001.files()
        """
        return self._api.concepts.files(self.dataset_id, self.type, self)

    def link(self, relationship_type, destination, values=dict()):
        """
        Creates a link between this record and another ``Record`` or ``DataPackage``

        Args:
            relationship_type (RelationshipType, str): type of relationship to create
            destination (Record, DataPackage):         ``Record`` or ``DataPackage`` that is related to the instance
            values (dict, optional):                   values for properties definied in the Relationship's schema

        Returns:
            Relationship that defines the link

        Example:
            Create a link between a ``Record`` and a ``DataPackage``::

                mouse_001.link('from', eeg)

            Create a link between two ``Record``::

                mouse_001.link('located_at', lab_009)

            Create a link (with values) between a ``Record`` and a ``DataPackage``::

                mouse_001.link('from', eeg, {"date": datetime.datetime(1991, 02, 26, 07, 0)})
        """
        self._check_exists()
        assert isinstance(destination, (Record, DataPackage)), "destination must be object of type Record or DataPackage"

        # auto-create relationship type
        if isinstance(relationship_type, basestring):
            relationships = self._api.concepts.relationships.get_all(self.dataset_id)
            if relationship_type not in relationships:
                r = RelationshipType(dataset_id=self.dataset_id, name=relationship_type, description=relationship_type)
                self._api.concepts.relationships.create(self.dataset_id, r)

        return self._api.concepts.relationships.instances.link(self.dataset_id, relationship_type, self, destination, values)

    def link_many(self, relationship_type, destinations, values=None):
        self._check_exists()
        values = [dict() for _ in values] if values is None else values
        assert len(destinations)==len(values), "Length of values must match length of destinations"
        
        # auto-create relationship type
        if isinstance(relationship_type, basestring):
            relationships = self._api.concepts.relationships.get_all(self.dataset_id)
            if relationship_type not in relationships:
                r = RelationshipType(dataset_id=self.dataset_id, name=relationship_type, description=relationship_type)
                self._api.concepts.relationships.create(self.dataset_id, r)

        for destination, dvalues in zip(destinations, values):
            assert isinstance(destination, (Record, DataPackage)), "destination must be object of type Record or DataPackage"
            yield self._api.concepts.relationships.instances.link(self.dataset_id, relationship_type, self, destination, dvalues)

    @property
    def model(self):
        """
        The ``Model`` of the current record.

        Returns:
           A single ``Model``.
        """
        return self._api.concepts.get(self.dataset_id, self.type)

    def update(self):
        """
        Updates the values of the record on the platform (after modification).

        Example::

          mouse_001.set('name', 'Mickey')
          mouse_001.update()
        """
        self._check_exists()

        _update_self(self, self._api.concepts.instances.update(self.dataset_id, self))

    def delete(self):
        """
        Deletes the instance from the platform.

        Example::

          mouse_001.delete()
        """
        return self._api.concepts.instances.delete(self.dataset_id, self)

    def __repr__(self):
        return u"<Record type='{}' id='{}'>".format(self.type, self.id)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Relationships
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class RelationshipProperty(BaseModelProperty):
    def __repr__(self):
        return u"<RelationshipProperty name='{}' {}>".format(self.name, self.type)

class RelationshipValue(BaseModelValue):
    def __repr__(self):
        return u"<RelationshipValue name='{}' value='{}' {}>".format(self.name, self.value, self.type)


class RelationshipType(BaseModelNode):
    """
    Model for defining a relationships.
    """
    _object_key = ''
    _property_cls = RelationshipProperty

    def __init__(self, dataset_id, name, display_name=None, description=None, locked=False, *args, **kwargs):

        kwargs.pop('type', None)
        super(RelationshipType, self).__init__(dataset_id, name, display_name, description, locked, *args, **kwargs)

    def update(self):
        raise Exception("Updating Relationships is not available at this time.")
        #TODO: _update_self(self, self._api.concepts.relationships.update(self.dataset_id, self))

    # TODO: delete when update is supported, handled in super-class
    def add_property(self, name, display_name=None, data_type=basestring):
        raise Exception("Updating Relationships is not available at this time.")

    # TODO: delete when update is supported, handled in super-class
    def add_properties(self, properties):
        raise Exception("Updating Relationships is not available at this time.")

    def delete(self):
        raise Exception("Deleting Relationships is not available at this time.")
        #TODO: self._api.concepts.relationships.delete(self.dataset_id, self)

    def get_all(self):
        """
        Retrieves all instances of the relationship from the platform.

        Returns:
            List of ``Relationship``

        Example::

          belongs_to_relationships = belongs_to.get_all()
        """
        return self._api.concepts.relationships.instances.get_all(self.dataset_id, self)

    def get(self, id):
        """
        Retrieves an instance of the relationship by id from the platform.

        Args:
            id (int): the id of the instance

        Returns:
            A single ``Relationship``

        Example::

          mouse_001 = mouse.get(123456789)
        """
        return self._api.concepts.relationships.instances.get(self.dataset_id, id, self)

    def link(self, source, destination, values=dict()):
        """
        Links a ``Record`` to another ``Record`` or ``DataPackage`` using current relationship.

        Args:
            source (Record, DataPackage):      record or data package the relationship orginates from
            destination (Record, DataPackage): record or data package the relationship points to
            values (dict, optional):           values for properties defined in the relationship's schema

        Returns:
            The newly created ``Relationship``

        Example:
            Create a relationship between a ``Record`` and a ``DataPackage``::

                from_relationship.link(mouse_001, eeg)

            Create a relationship (with values) between a ``Record`` and a ``DataPackage``::

                from_relationship.link(mouse_001, eeg, {"date": datetime.datetime(1991, 02, 26, 07, 0)})
        """
        self._check_exists()
        self._validate_values_against_schema(values)
        return self._api.concepts.relationships.instances.link(self.dataset_id, self, source, destination, values)

    def create_many(self, *item_list):
        """
        Create multiple relationships between ``Records`` using current relationship.

        Args:
            value_list (list): Array of dictionaries corresponding to relationships to be created.
                               Each relationship should be a dictionary containing 'source', 'destination',
                               and optional 'values' keys, where 'values' is also a dictionary.

        Returns:
            Array of newly created ``Relationships``s

        """
        self._check_exists()

        # Check sources and destinations
        for value in item_list:
            assert isinstance(value['destination'], (Record, DataPackage)), 'destination must be object of type Record or DataPackage'
            assert isinstance(value['source'], (Record, DataPackage)), 'source must be object of type Record or DataPackage'
            assert value['relationship_type']==self.type, u'RelationshipType type of items need to match relationship type: "{}"'.format(self.type)

        li_list = [
            Relationship(
                dataset_id  = self.dataset_id,
                type        = item['relationship_type'],
                source      = item['source'],
                destination = item['destination'],
                values     = [
                    dict(
                        name=k,
                        value=v,
                        dataType=self.schema.get(k).type
                    )
                    for k,v in item.get('values', {}).items()
                ]
            )
            for item in item_list
        ]

        return self._api.concepts.relationships.instances.create_many(self.dataset_id, self, *li_list)

    def as_dict(self):
        d = super(RelationshipType, self).as_dict()
        d['type'] = 'relationship'

        return d

    def __repr__(self):
        return u"<RelationshipType type='{}' id='{}'>".format(self.type, self.id)


class Relationship(BaseRecord):
    """
    A single instance of a ``RelationshipType``.
    """
    _object_key = ''

    def __init__(self, dataset_id, type, source, destination, *args, **kwargs):
        assert isinstance(source,  (Record, basestring)), "source must be Model or UUID"
        assert isinstance(destination, (Record, basestring)), "destination must be Model or UUID"

        if isinstance(source, Record):
            source = source.id
        if isinstance(destination, Record):
            destination = destination.id

        self.source = source
        self.destination = destination

        kwargs.pop('schemaRelationshipId', None)
        super(Relationship, self).__init__(dataset_id, type, *args, **kwargs)

    def relationship(self):
        """
        Retrieves the relationship definition of this instance from the platform

        Returns:
           A single ``RelationshipType``.
        """
        return self._api.concepts.relationships.get(self.dataset_id, self.type)

    # TODO: delete when update is supported, handled in super-class
    def set(self, name, value):
        raise Exception("Updating a Relationship is not available at this time.")

    def update(self):
        raise Exception("Updating a Relationship is not available at this time.")
        #TODO: _update_self(self, self._api.concepts.relationships.instances.update(self.dataset_id, self))

    def delete(self):
        """
        Deletes the instance from the platform.

        Example::

          mouse_001_eeg_link.delete()
        """
        return self._api.concepts.relationships.instances.delete(self.dataset_id, self)

    @classmethod
    def from_dict(cls, data, *args, **kwargs):
        d = dict(
            source  = data.pop('from', None),
            destination = data.pop('to', None),
            **data
        )
        item = super(Relationship, cls).from_dict(d, *args, **kwargs)
        return item

    def as_dict(self):
        d = super(Relationship, self).as_dict()
        d['to'] = self.destination
        d['from'] = self.source

        return d

    def __repr__(self):
        return u"<Relationship type='{}' id='{}' source='{}' destination='{}'>".format(self.type, self.id, self.source, self.destination)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Proxies
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class ProxyInstance(BaseRecord):
    _object_key = ''

    def __init__(self, dataset_id, type, *args, **kwargs):
        super(ProxyInstance, self).__init__(dataset_id, type, *args, **kwargs)

    def item(self):
        if self.type == 'proxy:package':
            package_id = self.get('id')
            return self._api.packages.get(package_id)
        else:
            raise Exception("unsupported proxy type: {}".format(self.type))

    def update(self):
        raise Exception("Updating a ProxyInstance is not available at this time.")

    def set(self, name, value):
        raise Exception("Updating a ProxyInstance is not available at this time.")

    def __repr__(self):
        return u"<ProxyInstance type='{}' id='{}'>".format(self.type, self.id)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Model/Relation Instance Sets
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class BaseInstanceList(list):
    _accept_type = None

    def __init__(self, type, *args, **kwargs):
        super(BaseInstanceList, self).__init__(*args, **kwargs)
        assert isinstance(type, self._accept_type), "type must be type {}".format(self._accept_type)
        self.type = type

    def as_dataframe(self):
        pass


class RecordSet(BaseInstanceList):
    _accept_type = Model

    def as_dataframe(self):
        """
        Converts the list of ``Record``s to a pandas DataFrame

        Returns:
          pd.DataFrame
        """
        cols = self.type.schema.keys()
        data = []
        for instance in self:
            data.append(instance.values)
        df = pd.DataFrame(data=data, columns=cols)
        return df


class RelationshipSet(BaseInstanceList):
    _accept_type = RelationshipType

    def as_dataframe(self):
        """
        Converts the list of ``Relationship`` to a pandas DataFrame

        Returns:
          pd.DataFrame

        Note:
          In addition to the values in each relationship instance, the DataFrame
          contains three columns that describe each instance:
            ``__source__``: ID of the instance's source
            ``__destination__``: ID of the instance's destination
            ``__type__``: Type of relationship that the instance is
        """
        cols = ['__source__', '__destination__', '__type__']
        cols.extend(self.type.schema.keys())

        data = []
        for instance in self:
            d = {}
            d['_type'] = self.type.type
            d['_source'] = instance.source
            d['_destination'] = instance.destination

            for name, value in instance.values.items():
                d[name] = value

            data.append(d)

        df = pd.DataFrame(data=data, columns=cols)
        return df
