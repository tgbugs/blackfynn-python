# -*- coding: utf-8 -*-

import logging
import datetime
import os

logging.basicConfig()
log = logging.getLogger('blackfynn')
log.setLevel(os.environ.get('BLACKFYNN_LOG_LEVEL', 'INFO'))

# data type helpers

def value_as_type(value, dtype):
    try:
        if dtype == 'string':
            return unicode(value)
        elif dtype == 'integer':
            return int(value)
        elif dtype == 'double':
            return float(value)
        elif dtype == 'date':
            return infer_epoch_msecs(value)
        if dtype == 'boolean':
            return value.lower()=='true'
    except:
        raise Exception('Unable to set value={} as type {}'.format(value, dtype))


def get_data_type(v):
    """
    Infers type from value. Returns tuple of (type, value)
    """
    if isinstance(v, datetime.datetime):
        return ("date", msecs_since_epoch(v))
    elif isinstance(v, bool):
        return ("boolean", str(v).lower())
    elif isinstance(v, float):
        return ("double", v)
    elif isinstance(v, (int,long)):
        return ("integer", v)
    else:
        # infer via casting
        if is_integer(v):
            return ("integer", int(v))
        elif is_decimal(v):
            return ("double", float(v))
        else:
            return ("string", str(v))

def is_integer(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

def is_decimal(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

# time-series helpers

def infer_epoch_msecs(thing):
    if isinstance(thing, datetime.datetime):
        return msecs_since_epoch(thing)
    elif isinstance(thing, (int,long,float)):
        # assume milliseconds
        return long(thing)
    elif isinstance(thing, basestring):
        # attempt to convert to msec integer
        return long(thing)
    else:
        raise Exception("Cannot parse date")

def infer_epoch(thing):
    if isinstance(thing, datetime.datetime):
        return usecs_since_epoch(thing)
    elif isinstance(thing, (int,long,float)):
        # assume microseconds
        return long(thing)
    else:
        raise Exception("Cannot parse date")

def secs_since_epoch(the_time):
    the_time = the_time.replace(tzinfo=None)
    # seconds from epoch (float)
    return (the_time-datetime.datetime.utcfromtimestamp(0)).total_seconds()

def msecs_since_epoch(the_time):
    # milliseconds from epoch (integer)
    return long(secs_since_epoch(the_time)*1000)

def usecs_since_epoch(the_time):
    # microseconds from epoch (integer)
    return long(secs_since_epoch(the_time)*1e6)

def usecs_to_datetime(us):
    # convert usecs since epoch to proper datetime object
    return datetime.datetime.utcfromtimestamp(0) + datetime.timedelta(microseconds=long(us))
