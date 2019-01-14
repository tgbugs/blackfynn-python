# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function
from future.utils import string_types

import datetime
import math


from blackfynn.api.base import APIBase
from blackfynn.models.workspace import Workspace
from blackfynn.utils import find_by_name


class WorkspacesAPI(APIBase):
    """
    Interface for managing workspaces on the platform.
    """
    base_uri='/workspaces'
    name = 'workspaces'


    def get(self, name_or_id):
        workspace_id = name_or_id
        if isinstance(name_or_id, (string_types)):
            workspaces = self.get_all()
            match = find_by_name(workspaces, name_or_id)
            # return the match if one was found, otherwise there was no
            # workspace with that name
            if match is not None:
                return match
            return

        resp = self._get(self._uri('/{id}', id=workspace_id))
        return Workspace.from_dict(resp, api=self.session)

    def get_all(self):
        resp = self._get(self._uri('/'))
        return [Workspace.from_dict(workspace, api=self.session) for workspace in resp]

    def create(self, workspace):
        """
        Create a workspace on the platform
        """
        if self.get(workspace.name) is not None:
            raise Exception("Workspace with name {} already exists".format(workspace.name))

        resp = self._post('', json=workspace.as_dict())
        return Workspace.from_dict(resp, api=self.session)

    def update(self, workspace):
        """
        Update a workspace on the platform
        """
        id = self._get_id(workspace)
        resp = self._put( self._uri('/{id}', id=id), json=workspace.as_dict())
        return Workspace.from_dict(resp, api=self.session)

    def delete(self, workspace):
        """
        Delete a workspace on the platform
        """
        id = self._get_id(workspace)
        resp = self._del(self._uri('/{id}', id=id))
        workspace.id = None
        return resp

