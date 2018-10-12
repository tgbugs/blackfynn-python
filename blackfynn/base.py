# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function
from builtins import dict, object

import base64
import json

import requests
from requests import Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# blackfynn
import blackfynn.log as log
from blackfynn.models import User


class UnauthorizedException(Exception):
    pass


class BlackfynnRequest(object):
    def __init__(self, func, uri, *args, **kwargs):
        self._func = func
        self._uri = uri
        self._args = args
        self._kwargs = kwargs
        self._response = None

        self._logger = log.get_logger('blackfynn.base.BlackfynnRequest')

    def _handle_response(self, resp):
        self._logger.debug(u"resp = {}".format(resp))
        self._logger.debug(u"resp.content = {}".format(resp.text)) # decoded unicode
        if resp.status_code in [requests.codes.forbidden, requests.codes.unauthorized]:
            raise UnauthorizedException()

        if not resp.status_code in [requests.codes.ok, requests.codes.created]:
            resp.raise_for_status()
        try:
            # return object from json
            resp.data = json.loads(resp.text)
        except:
            # if not json, still return response content
            resp.data = resp.text

    def call(self, timeout=None):
        self._response = self._func(self._uri, *self._args, timeout=timeout, **self._kwargs)
        self._handle_response(self._response)
        return self._response


class ClientSession(object):
    def __init__(self, settings):
        self._host = settings.api_host
        self._streaming_host = settings.streaming_api_host
        self._concepts_host = settings.concepts_api_host
        self._api_token = settings.api_token
        self._api_secret = settings.api_secret

        self._logger = log.get_logger('blackfynn.base.ClientSession')

        self._session = None
        self._token = None
        self._secret = None
        self._context = None
        self._organization = None
        self.profile = None
        self.settings = settings

    def authenticate(self, organization = None):
        """
        API token is used to authentiate against the Blackfynn platform. An API
        session token is returned from the API call and is used for all subsequent
        API calls.
        """
        # make authentication request
        session_response = self._post('/account/api/session', json=dict(tokenId = self._api_token, secret = self._api_secret))

        # parse response, set session
        self.token = session_response['session_token']
        self.profile = User.from_dict(self._get('/user/'))

        if organization is None:
            organization = session_response.get('organization')

        self._set_org_context(organization)

    @property
    def token(self):
        return self._token

    @token.setter
    def token(self, value):
        self._token = value
        self._set_auth(value)

    def _set_org_context(self, organization_id):
        self._organization = organization_id
        self._session.headers['X-ORGANIZATION-ID'] = organization_id

    def _set_auth(self, session_token):
        self._session.headers.update({
            'X-SESSION-ID': session_token,
            'Authorization': 'Bearer {}'.format(session_token)
        })

    @property
    def session(self):
        """
        Make requests-futures work within threaded/distributed environment.
        """
        if self._session is None:
            self._session = Session()
            self._set_auth(self._token)

            # Enable retries via urllib
            adapter = HTTPAdapter(
                max_retries=Retry(
                    total=self.settings.max_request_timeout_retries,
                    backoff_factor=.5,
                    status_forcelist=[502, 503, 504] # Retriable errors (but not POSTs)
                )
            )
            self._session.mount('http://', adapter)
            self._session.mount('https://', adapter)

        return self._session

    def _make_request(self, func, uri, *args, **kwargs):
        self._logger.debug('~'*60)
        self._logger.debug("uri = {} {}".format(func.__func__.__name__, uri))
        self._logger.debug("args = {}".format(args))
        self._logger.debug("kwargs = {}".format(kwargs))
        self._logger.debug("headers = {}".format(self.session.headers))
        return BlackfynnRequest(func, uri, *args, **kwargs)

    def _call(self, method, endpoint, base='', *args, **kwargs):
        if method == 'get':
            func = self.session.get
        elif method == 'put':
            func = self.session.put
        elif method == 'post':
            func = self.session.post
        elif method == 'delete':
            func = self.session.delete

        # serialize data
        if 'data' in kwargs:
            kwargs['data'] = json.dumps(kwargs['data'])

        # we might specify a different host
        if 'host' in kwargs:
            host = kwargs['host']
            kwargs.pop('host')
        else:
            host = self._host

        # call endpoint
        uri = self._uri(endpoint, base=base, host=host)
        req = self._make_request(func, uri, *args, **kwargs)
        resp = self._get_response(req)

        return resp.data

    def _uri(self, endpoint, base, host=None):
        if host is None:
            host = self._host
        return '{}{}{}'.format(host, base, endpoint)

    def _get(self, endpoint, *args, **kwargs):
        return self._call('get', endpoint, *args, **kwargs)

    def _post(self, endpoint, *args, **kwargs):
        return self._call('post', endpoint, *args, **kwargs)

    def _put(self, endpoint, *args, **kwargs):
        return self._call('put', endpoint, *args, **kwargs)

    def _del(self, endpoint, *args, **kwargs):
        return self._call('delete', endpoint, *args, **kwargs)

    def _get_response(self, req):
        try:
            return req.call(timeout=self.settings.max_request_time)
        except UnauthorizedException as e:
            if self._token is None:
                raise e

            # try to refresh the session and re-request
            self.authenticate(self._organization)
            return req.call(timeout=self.settings.max_request_time)

    def register(self, *components):
        """
        Register API component with session. Components should all be of
        APIBase type and have a name and base_uri property.

        The registered component will have reference to base session to
        make higher-level calls outside of its own scope, if needed.
        """
        # initialize
        for component in components:
            c = component(session=self)
            assert len(component.name) > 1, "Invalid API component name"
            # component is accessible via session.(name)
            self.__dict__.update({ component.name: c })

    @property
    def headers(self):
        return self.session.headers
