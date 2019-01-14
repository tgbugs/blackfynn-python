# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function
from builtins import object, zip
from future.utils import as_native_str, string_types, PY2

import os
import io
import json
import requests
import tempfile
import pyarrow
import pandas as pd

from blackfynn.models import BaseNode
from blackfynn.utils import find_by_name

if PY2:
    file_types = (file, io.IOBase)
else:
    file_types = (io.IOBase)


class Workspace(BaseNode):
    """ Represents a Workspace object on the Blackfynn platform """
    _object_key = None

    def __init__(self, name, description=None, **kwargs):
        kwargs.pop('package_type', None)
        kwargs.pop('type', None)
        super(Workspace, self).__init__()
        self.name = name
        self.description = description or ''

    def __eq__(self, other):
        return self.id == other.id

    @as_native_str()
    def __repr__(self):
        return u"<Workspace name='{}' id='{}'>".format(self.name, self.id)

    def create_view(self, dataset, name, root, include, create_snapshot=True):
        """
        Create a graph view rooted at the given model

        Args:
            dataset: `Dataset` object or dataset integer id
            name (str): name of the view
            root (str or Model): model that roots the view
            include (list): list of related models to include in the view

        Returns:
            GraphViewDefinition
        """
        view = self._api.analytics.create_view(self, dataset,
                                               name, root, include)
        if create_snapshot:
            view.create_snapshot()
        return view

    def get_view_definition(self, name_or_id):
        """
        Get a graph view definition with a given name or id

        Args:
            name_or_id (str): Name or ID of the view

        Returns:
            GraphViewDefinition
        """
        try:
            # by id
            view = self._api.analytics.get_view(self, name_or_id)
        except:
            # by name
            views = self._api.analytics.get_all_views(self)
            view = find_by_name(views, name_or_id)
            if view is None:
                raise Exception("View '{}' not found".format(name_or_id))

        return view

    def views(self):
        """
        Returns:
            List of all graph views defined in the workspace
        """
        return self._api.analytics.get_all_views(self)

    def queries(self):
        """
        Returns:
            List of all named queries defined in the workspace
        """
        return self._api.analytics.get_all_named_queries(self)

    def delete_contents(self):
        """ Deletes all contents of the current workspace """
        return self._api.analytics.delete_contents(self)

    def delete(self):
        """ Delete a workspace and all of its contents """
        return self._api.workspaces.delete(self)

    def create_named_query(self, name, query):
        """ Create a new named query in the current workspace

        Args:
            name (str): Name of the query
            query (str): Query string

        Returns:
            NamedQuery
        """
        return self._api.analytics.create_named_query(self, name, query)

    def get_named_query(self, name_or_id):
        """ Retreive a named query

        Args:
            name_or_id (str): Name or ID of the named query

        Returns:
            NamedQuery
        """
        try:
            # by id
            query = self._api.analytics.get_named_query(self, name_or_id)
        except:
            # by name
            queries = self._api.analytics.get_all_named_queries(self)
            query = find_by_name(queries, name_or_id)
            if query is None:
                # TODO: Start raising custom exceptions
                raise Exception("NamedQuery '{}' not found".format(name_or_id))
        return query

    def get_all_named_queries(self):
        """ Retrieve all named queries in a workspace """
        return self._api.analytics.get_all_named_queries(self)

    def delete_all_named_queries(self):
        """ Deleted all named queries in a workspace """
        return self._api.analytics.delete_all_named_queries(self)

    def as_dict(self):
        return dict(
            name = self.name,
            description = self.description,
        )


class NamedQuery(BaseNode):
    """ Represents a NamedQuery object within a Workspace """
    _object_key = None

    def __init__(self, name, query, workspace_id, *args, **kwargs):
        self.name = name
        self.query = query
        self.workspace_id = workspace_id
        self.created_at = kwargs.pop("createdAt", None)

        super(NamedQuery, self).__init__(*args, **kwargs)

    def delete(self):
        self._check_exists()
        self._api.analytics.delete_named_query(self.workspace_id, self)

    def as_dict(self):
        return dict(
            id = self.id,
            name = self.name,
            workspace_id = self.workspace_id,
            query = self.query,
            created_at = self.created_at
        )

    @as_native_str()
    def __repr__(self):
        return u"<NamedQuery name='{}' id='{}'>".format(self.name, self.id)


class GraphViewDefinition(BaseNode):
    _object_key = None

    def __init__(self, name, root_model, included_models, workspace_id,
                 dataset_id, *args, **kwargs):
        self.name = name
        self.root_model = root_model
        self.included_models = included_models
        self.workspace_id = workspace_id
        self.dataset_id = dataset_id
        self.created_at = kwargs.pop("createdAt", None)
        super(GraphViewDefinition, self).__init__(*args, **kwargs)

    def create_snapshot(self, batch_size=100):
        """
        Capture the current state of the data in the view.
        """
        return self._api.analytics.create_snapshot(self.workspace_id, self, batch_size)

    def snapshots(self, status='ready'):
        """
        All snapshots (versions) of this view.

        Args:
            status (str):
                Filter snapshots based on status. Value must be
                one of 'processing', 'failed', 'ready', or 'any'/None.

        Returns:
            List of GraphViewSnapshot objects
        """
        status = None if status == 'any' else status
        if status not in GraphViewSnapshot._valid_snapshot_status + [None]:
            raise Exception("Status must be one of {valid}".format(
                    valid = GraphViewSnapshot._valid_snapshot_status + ['any', None]
            ))
        instances = self._api.analytics.get_all_snapshots(self.workspace_id, self, status)
        return sorted(instances, key=lambda x: x.created_at)

    def get_snapshot(self, id):

        """
        Get specific snapshot of this view.

        Args:
            id (str):
                ID of snapshot to return
            status (str):
                Filter snapshots based on status. Value must be
                one of 'processing', 'failed', 'ready', or 'any'/None.

        Returns:
            GraphViewSnapshot object
        """
        return self._api.analytics.get_snapshot(self.workspace_id, self, id)

    def delete_snapshot(self, id):
        """ Delete the given snapshot

        Args:
            id: (str):
                ID of snapshot to delete
        """
        return self._api.analytics.delete_snapshot(self.workspace_id, id)

    def latest(self, status='ready'):
        """
        Return most recent snapshot of the view.

        Args:
            status (str):
                Filter snapshots based on status. Value must be
                one of 'processing', 'failed', 'ready', or 'any'/None.

        Returns:
            GraphViewSnapshot object
        """
        status = None if status == 'any' else status
        if status not in GraphViewSnapshot._valid_snapshot_status + [None]:
            raise Exception("Status must be one of {valid}".format(
                    valid = GraphViewSnapshot._valid_snapshot_status + ['any', None]
            ))
        return self._api.analytics.get_latest_snapshot(self.workspace_id, self, status=status)

    def delete(self):
        """
        Delete the view.
        """
        self._check_exists()
        r = self._api.analytics.delete_view(self.workspace_id, self)
        self.id = None
        return r

    def as_dict(self):
        return dict(
            id = self.id,
            name = self.name,
            workspace_id = self.workspace_id,
            dataset_id = self.dataset_id,
            root_model = self.root_model,
            included_models = self.included_models,
            created_at = self.created_at
        )

    @as_native_str()
    def __repr__(self):
        return u"<GraphViewDefinition name='{}' id='{}'>".format(self.name, self.id)


class GraphViewSnapshot(BaseNode):
    _object_key = None
    _valid_snapshot_status = ['failed', 'processing', 'ready']

    def __init__(self, view, workspace_id, *args, **kwargs):
        self.view = view
        self.workspace_id = workspace_id
        self.created_at = kwargs.pop("createdAt", None)
        self.status = kwargs.pop("status", None)

        super(GraphViewSnapshot, self).__init__(*args, **kwargs)

    def as_dataframe(self, columns=None):
        """
        Returns:
            pd.DataFrame:
        """
        url = self._api.analytics.get_presigned_url(self.workspace_id, self, format='parquet')

        filename = os.path.join(tempfile.gettempdir(), '{}.parquet'.format(self.id))
        self.download(filename, format='parquet')

        try:
            # TODO: allow for wildcard columns, e.g. `patient.*`
            df = pd.read_parquet(filename, columns=columns)
        except pyarrow.ArrowIOError:
            with io.open(filename) as f:
                self._check_response(f.read())
            raise
        finally:
            try:
                os.remove(filename)
            except:
                pass

        return df

    def download(self, location, format='parquet'):
        """
        Download the snapshot dataframe as a file.

        Args:
            location (str or file-like object):
                Location to save snapshot dataframe. Can be either
                filesystem path string or some file-like
                object (in-memory or on-disk).

            format (str):
                Format of file. Can be either 'parquet', 'json'.
        """
        url = self._api.analytics.get_presigned_url(self.workspace_id, self, format=format)

        def download_file_contents(response, f):

            for chunk in response.iter_content(chunk_size=16384):
                if chunk:
                    f.write(chunk)

        with requests.get(url, stream=True) as r:
            if isinstance(location, string_types):
                with io.open(location, 'wb') as f:
                    download_file_contents(r, f)
            elif isinstance(location, file_types):
                download_file_contents(r, location)
            else:
                raise Exception('location must be file path (str) or file-like object')

    def delete(self):
        """ Deletes the snapshot """
        return self._api.analytics.delete_snapshot(self.workspace_id, self.id)

    def as_json(self):
        """
        Returns:
g           Data as a JSON structure
        """
        url = self._api.analytics.get_presigned_url(self.workspace_id, self, format='json')
        resp = requests.get(url)

        try:
            return resp.json()
        except json.JSONDecodeError:
            self._check_response(resp.text)

    def as_dict(self):
        return dict(
            id = self.id,
            created_at = self.created_at,
            view = self.view.as_dict(),
            status = self.status,
        )

    def _check_response(self, content):
        if '<Code>NoSuchKey</Code>' in content:
            raise Exception("View has not finished processing. Try again soon.")

    @as_native_str()
    def __repr__(self):
        if isinstance(self.view, GraphViewDefinition):
            view_str = self.view.name
        else:
            view_str = self.view
        return u"<GraphViewSnapshot view='{}' created='{}' id='{}'>".format(
            view_str, self.created_at, self.id)

