# -*- coding: utf-8 -*-

from __future__ import absolute_import, division, print_function
from builtins import object, zip
from future.utils import as_native_str, string_types, PY2

import requests

from blackfynn.api.base import APIBase
from blackfynn.models.workspace import GraphViewSnapshot, GraphViewDefinition, \
    NamedQuery

class AnalyticsAPI(APIBase):
    base_uri = '/analytics'
    name = 'analytics'

    def _with_kwargs(self, workspace, dataset=None, view=None, instance=None,
                     query_obj=None, **kwargs):
        kwargs.update({
            'orgId': self._get_int_id(self.session._context),
            'workspaceId': self._get_id(workspace)
        })

        if dataset is not None:
            kwargs['datasetId'] = self._get_int_id(dataset)

        if view is not None:
            kwargs['view'] = view
            kwargs['graphViewId'] = self._get_id(view)

        if instance is not None:
            kwargs['graphViewInstanceId'] = self._get_id(instance)

        if query_obj is not None:
            kwargs['queryId'] = self._get_id(query_obj)

        return kwargs

    def create_view(self, workspace, dataset, name, root, include):
        uri = self._uri('/organizations/{orgId}/workspaces/{workspaceId}/views/definitions?dataset_id={datasetId}',
                        **self._with_kwargs(workspace, dataset=dataset))

        resp = self._post(uri, json={
            'name': name,
            'rootModel': root,
            'includedModels': include,
        })
        resp = self._with_kwargs(workspace, dataset=dataset, **resp)
        return GraphViewDefinition.from_dict(resp, api=self.session)

    def get_view(self, workspace, view):
        uri = self._uri('/organizations/{orgId}/workspaces/{workspaceId}/views/definitions/{graphViewId}',
                        **self._with_kwargs(workspace, view=view))
        resp = self._get(uri)
        resp = self._with_kwargs(workspace, **resp)
        return GraphViewDefinition.from_dict(resp, api=self.session)

    def delete_view(self, workspace, view):
        uri = self._uri('/organizations/{orgId}/workspaces/{workspaceId}/views/definitions/{graphViewId}',
                        **self._with_kwargs(workspace, view=view))
        return self._del(uri)

    def get_all_views(self, workspace):
        uri = self._uri('/organizations/{orgId}/workspaces/{workspaceId}/views/definitions',
                        **self._with_kwargs(workspace))
        resp = self._get(uri)
        return [GraphViewDefinition.from_dict(self._with_kwargs(workspace, **r), api=self.session)
                for r in resp]

    def create_snapshot(self, workspace, view, batch_size=100):
        uri = self._uri('/organizations/{orgId}/workspaces/{workspaceId}/views/definitions/{graphViewId}/snapshots?dataset_id={dataset_id}&batchSize={batch_size}',
                        batch_size=batch_size, dataset_id=view.dataset_id,
                        **self._with_kwargs(workspace, view=view))
        resp = self._post(uri)
        resp = self._with_kwargs(workspace, view=view, **resp)
        snapshot = GraphViewSnapshot.from_dict(resp, api=self.session)
        return snapshot

    def get_snapshot(self, workspace, view, instance):
        uri = self._uri('/organizations/{orgId}/workspaces/{workspaceId}/views/snapshots/{graphViewInstanceId}',
                        **self._with_kwargs(workspace, instance=instance))
        try:
            resp = self._get(uri)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return None
            raise

        resp = self._with_kwargs(workspace, view=view, **resp)
        return GraphViewSnapshot.from_dict(resp, api=self.session)

    def get_latest_snapshot(self, workspace, view, status=None):
        params = dict(status=status) if status is not None else None
        uri = self._uri('/organizations/{orgId}/workspaces/{workspaceId}/views/definitions/{graphViewId}/snapshots/latest',
                        **self._with_kwargs(workspace, view=view))
        try:
            resp = self._get(uri, params=params)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return None
            raise

        resp = self._with_kwargs(workspace, view=view, **resp)
        return GraphViewSnapshot.from_dict(resp, api=self.session)

    def get_all_snapshots(self, workspace, view, status=None):
        params = dict(status=status) if status is not None else None
        uri = self._uri('/organizations/{orgId}/workspaces/{workspaceId}/views/definitions/{graphViewId}/snapshots',
                        **self._with_kwargs(workspace, view=view))
        try:
            resp = self._get(uri, params=params)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                return []
            raise

        return [GraphViewSnapshot.from_dict(self._with_kwargs(workspace, view=view, **r), api=self.session)
                for r in resp]

    def get_presigned_url(self, workspace, instance, format='parquet'):

        uri = self._uri('/organizations/{orgId}/workspaces/{workspaceId}/views/snapshots/{graphViewInstanceId}/url?format={format}',
                        format=format, **self._with_kwargs(workspace, instance=instance))
        return self._get(uri)

    def delete_snapshot(self, workspace, instance):
        uri = self._uri('/organizations/{orgId}/workspaces/{workspaceId}/views/snapshots/{graphViewInstanceId}',
                        **self._with_kwargs(workspace, instance=instance))
        return self._del(uri)

    def delete_contents(self, workspace):
        uri = self._uri('/organizations/{orgId}/workspaces/{workspaceId}',
                        **self._with_kwargs(workspace))
        return self._del(uri)

    def create_named_query(self, workspace, name, query):
        uri = self._uri('/organizations/{orgId}/workspaces/{workspaceId}/queries/',
                        **self._with_kwargs(workspace))

        resp = self._post(uri, json={
            'name': name,
            'query': query,
        })
        resp = self._with_kwargs(workspace, **resp)
        return NamedQuery.from_dict(resp, api=self.session)

    def get_named_query(self, workspace, query):
        uri = self._uri('/organizations/{orgId}/workspaces/{workspaceId}/queries/{queryId}',
                        **self._with_kwargs(workspace, query_obj=query))
        resp = self._get(uri)
        resp = self._with_kwargs(workspace, **resp)
        return NamedQuery.from_dict(resp, api=self.session)

    def get_all_named_queries(self, workspace):
        uri = self._uri('/organizations/{orgId}/workspaces/{workspaceId}/queries/',
                        **self._with_kwargs(workspace))
        resp = self._get(uri)
        return [NamedQuery.from_dict(self._with_kwargs(workspace, **r), api=self.session)
                for r in resp]

    def delete_named_query(self, workspace, query):
        uri = self._uri('/organizations/{orgId}/workspaces/{workspaceId}/queries/{queryId}',
                        **self._with_kwargs(workspace, query_obj=query))
        return self._del(uri)

    def delete_all_named_queries(self, workspace):
        uri = self._uri('/organizations/{orgId}/workspaces/{workspaceId}/queries/',
                        **self._with_kwargs(workspace))
        return self._del(uri)
