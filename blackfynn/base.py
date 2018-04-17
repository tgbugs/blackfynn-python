# -*- coding: utf-8 -*-

import json
import base64
import requests
from concurrent.futures import TimeoutError
from requests_futures.sessions import FuturesSession

# blackfynn
from blackfynn.utils import log
from blackfynn.models import User

class UnauthorizedException(Exception):
    pass


class BlackfynnRequest(object):
    def __init__(self, func, uri, *args, **kwargs):
        self._func = func
        self._uri = uri
        self._args = args
        self._kwargs = kwargs
        self._request = None
        self.call()

    def _handle_response(self, sess, resp):
        log.debug("resp = {}".format(resp))
        log.debug("resp.content = {}".format(resp.content))
        if resp.status_code in [requests.codes.forbidden, requests.codes.unauthorized]:
            raise UnauthorizedException()

        if not resp.status_code in [requests.codes.ok, requests.codes.created]:
            resp.raise_for_status()
        try:
            # return object from json
            resp.data = json.loads(resp.content)
        except:
            # if not json, still return response content
            resp.data = resp.content

    def call(self):
        self._request = self._func(self._uri, background_callback=self._handle_response, *self._args, **self._kwargs)
        return self

    def result(self,*args, **kwargs):
        return self._request.result(*args, **kwargs)


class ClientSession(object):
    def __init__(self, settings):
        self._host = settings.api_host
        self._streaming_host = settings.streaming_api_host
        self._concepts_host = settings.concepts_api_host
        self._api_token = settings.api_token
        self._api_secret = settings.api_secret

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
        if not hasattr(self._session, 'session'):
            self._session = FuturesSession(max_workers=4)
            self._set_auth(self._token)

        return self._session

    def _make_call(self, func, uri, *args, **kwargs):
        log.debug('~'*60)
        log.debug("uri = {} {}".format(func.__func__.func_name, uri))
        log.debug("args = {}".format(args))
        log.debug("kwargs = {}".format(kwargs))
        log.debug("headers = {}".format(self.session.headers))
        return BlackfynnRequest(func, uri, *args, **kwargs)

    def _call(self, method, endpoint, base='', async=False, *args, **kwargs):
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
        req = self._make_call(func, uri, *args, **kwargs)

        if async:
            return req
        else:
            return self._get_response(req)

    def _uri(self, endpoint, base, host=None):
        if host is None:
            host = self._host
        return '{}{}{}'.format(host, base, endpoint)

    def _get(self, endpoint, async=False, *args, **kwargs):
        return self._call('get', endpoint, async=async, *args, **kwargs)

    def _post(self, endpoint, async=False, *args, **kwargs):
        return self._call('post', endpoint, async=async, *args, **kwargs)

    def _put(self, endpoint, async=False, *args, **kwargs):
        return self._call('put', endpoint, async=async, *args, **kwargs)

    def _del(self, endpoint, async=False, *args, **kwargs):
        return self._call('delete', endpoint, async=async, *args, **kwargs)

    def _get_result(self, req, count=0):
        try:
            resp = req.result(timeout=self.settings.max_request_time)
        except TimeoutError as e:
            if count < self.settings.max_request_timeout_retries:
                # timeout! trying again...
                resp = self._get_result(req.call(), count=count+1)
        except UnauthorizedException as e:
            # try refreshing the session
            if self._token is not None and count==0:
                self.authenticate(self._organization)
                # re-request
                resp = self._get_result(req.call(), count=count+1)
            else:
                raise e
        return resp

    def _get_response(self, req):
        resp = self._get_result(req)
        return resp.data

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
