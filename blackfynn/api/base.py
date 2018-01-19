
import urllib

# blackfynn
from blackfynn.models import get_package_class

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Base class
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class APIBase(object):
    host = None
    base_uri = ''
    name = ''

    def __init__(self, session):
        """
        Base class to be used by all API components.
        """
        # api session
        self.session = session

    def _get_id(self, thing):
        """
        Get ID for object. Assumes string is already ID.
        """
        if isinstance(thing, (basestring, int, long)):
            return thing
        elif thing is None:
            return None
        else:
            return thing.id

    def _get_package_from_data(self, data):
        # parse json
        cls = get_package_class(data)
        pkg = cls.from_dict(data, api=self.session)

        return pkg

    def _uri(self, url_str, **kwvars):
        vals = {k:urllib.quote(str(var)) for k,var in kwvars.items()}
        return url_str.format(**vals)

    def _get(self, endpoint, async=False, base=None, host=None, *args, **kwargs):
        base = self.base_uri if base is None else base
        host = self.host     if host is None else host
        return self.session._call('get', endpoint, host=host, base=base, async=async, *args, **kwargs)

    def _post(self, endpoint, async=False, base=None, host=None, *args, **kwargs):
        base = self.base_uri if base is None else base
        host = self.host     if host is None else host
        return self.session._call('post', endpoint, host=host, base=base, async=async, *args, **kwargs)

    def _put(self, endpoint, async=False, base=None, host=None, *args, **kwargs):
        base = self.base_uri if base is None else base
        host = self.host     if host is None else host
        return self.session._call('put', endpoint, host=host, base=base, async=async, *args, **kwargs)

    def _del(self, endpoint, async=False, base=None, host=None, *args, **kwargs):
        base = self.base_uri if base is None else base
        host = self.host     if host is None else host
        return self.session._call('delete', endpoint, host=host, base=base, async=async, *args, **kwargs)

    def _get_response(self, req):
        return self.session._get_response(req)
