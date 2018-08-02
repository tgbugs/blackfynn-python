# -*- coding: utf-8 -*-

import re
import math
import datetime
import numpy as np
import pandas as pd
from types import NoneType
from itertools import islice, count
from concurrent.futures import ThreadPoolExecutor

import requests

# blackfynn
from blackfynn.api.base import APIBase
from blackfynn.streaming import TimeSeriesStream
from blackfynn.utils import (
    usecs_to_datetime, usecs_since_epoch, infer_epoch
)
from blackfynn.models import (
    File, TimeSeries,TimeSeriesChannel, TimeSeriesAnnotation,
    get_package_class, TimeSeriesAnnotation, TimeSeriesAnnotationLayer
)
from blackfynn.cache import get_cache

cache = None

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Helpers
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

def parse_timedelta(time):
    """
    Returns microseconds of time expression, where time can be of the forms:
     - string:  e.g. '1s', '5m', '3h'
     - delta:   datetime.timedelta object
    """
    if isinstance(time, basestring):
        # parse string into timedelta
        regex = re.compile(r'((?P<hours>\d*\.*\d+?)hr)?((?P<minutes>\d*\.*\d+?)m)?((?P<seconds>\d*\.*\d+?)s)?')
        parts = regex.match(time)
        if not parts:
            return
        parts = parts.groupdict()
        time_params = {}
        for (name, param) in parts.iteritems():
            if param:
                time_params[name] = float(param)
        time = datetime.timedelta(**time_params)

    if isinstance(time, datetime.timedelta):
        # return microseconds
        return long(time.total_seconds()*1e6)

    elif isinstance(time, (long, int, float)):
        # assume already in microseconds
        return time

vec_usecs_to_datetime = np.vectorize(usecs_to_datetime)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# TimeSeries Request
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class ChannelPage(object):
    def __init__(self, channel, page, settings, use_cache=True):
        self.channel   = channel
        self.page      = long(page)
        self.use_cache = use_cache

        page_size = settings.ts_page_size
        global cache
        if self.use_cache and cache is None:
            cache = get_cache(settings, start_compaction=True)
            page_size = cache.page_size

        # fixed page -- determined from epoch(0)
        pg_delta = channel._page_delta(page_size)
        self.start = long(self.page  * pg_delta)
        self.stop  = long(self.start + pg_delta)
        self.cache_exists = False

        # current future/response
        self.future = None
        self.data   = None

    def request(self, api):
        # check if page is cached
        if self.use_cache and cache.check_page(self.channel, self.page):
            # we (should) have cache, skip API request
            self.cache_exists = True
            return

        # make request: not using cache
        args = dict(
            # Note: uses streaming server
            host     = api._streaming_host,
            endpoint = '/ts/retrieve/continuous',
            base     = '',
            async    = True,
            params   = dict(
                channel = self.channel.id,
                limit   = '', # required by API
                session = api.headers.get('X-SESSION-ID'),
                start   = self.start,
                end     = self.stop)
        )
        self.future = api._get(**args)

    def get(self, api, update_cache_if_exists=False):
        if self.future is not None:
            # we're handling an API request/response
            self.data = self._get_response(api)

            # set cache!
            if self.use_cache and (not self.cache_exists or update_cache_if_exists):
                cache.set_page_data(self.channel, self.page, self.data, update=update_cache_if_exists)

        elif self.data is not None:
            # we've already got the result
            pass

        elif self.use_cache and self.cache_exists:
            # use existing cache entry
            self.data = cache.get_page_data(self.channel, self.page)

            if self.data is None:
                # cache may have disappeared, let's make API call
                self.use_cache = False
                self.cache_exists = False
                self.request(api)
                self.data = self.get(api, update_cache_if_exists=True)

        return self.data

    def _get_response(self, api, datetime_index=True):
        # handle API response, return data series
        resp  = api._get_response(self.future)
        self.future = None

        # handle data response
        times = np.array( [t[0] for t in resp] )
        data  = np.array( [d[1] for d in resp] )
        # fix -- sometimes API responds out-of-order
        order = np.argsort(times)
        times = times[order]
        data  = data[order]

        if datetime_index and len(times)>0:
            times = vec_usecs_to_datetime(times)

        # return pandas series
        return pd.Series( data=data, index=times, name=self.channel)


class ChannelIterator(object):
    """
    We make requests to API/cache using some fixed page-size, but the
    user typically wants data results in some specified "chunk size".
    This accumulates the data pages in order to serve the data back
    in the specified chunk size.
    """
    def __init__(self, channel, start, stop, chunk_time, api, use_cache=True):
        self.channel    = channel
        self.start      = start
        self.stop       = stop
        self.start_dt   = usecs_to_datetime(self.start)
        self.stop_dt    = usecs_to_datetime(self.stop)
        self.use_cache  = use_cache
        self.api        = api

        # page delta (usecs) for channel
        self.page_delta = channel._page_delta(api.settings.ts_page_size)

        # page iteration
        self.page_start = long(math.floor(self.start/(1.0*self.page_delta)))
        self.page_end   = long(math.ceil(self.stop /(1.0*self.page_delta)))

        # chunk iteration
        self.chunk_per_page = chunk_time is None
        # chunk over specfied time
        if not self.chunk_per_page:
            self.chunk_time = long(chunk_time) # in usecs
            self.chunk_size = long(channel.rate * self.chunk_time/1.0e6)
        self.chunk  = None
        self.offset = usecs_to_datetime(self.start)

    def get_chunks(self):
        # page size may be more/less than requested data

        self.chunk = pd.Series()
        pages = iter(np.arange(self.page_start, self.page_end))
        page = None
        while True:
            if self.chunk_per_page or \
               (page is None and len(self.chunk) < self.chunk_size):
                # get next page
                try: p = next(pages)
                except: break
                page = ChannelPage(
                        settings = self.api.settings,
                        channel   = self.channel,
                        page      = p,
                        use_cache = self.use_cache)
                page.request(self.api)
                data = page.get(self.api)
                # no more data
                if data is None: break
                # grow data series
                i_start = None
                i_stop  = None
                if page.start < self.start:
                    i_start = self.start_dt
                if page.stop >= self.stop:
                    i_stop = self.stop_dt
                else:
                    # more pages needed - reset
                    page = None
                data_slice = data.loc[i_start:i_stop]
                if self.chunk_per_page:
                    yield data_slice
                else:
                    self.chunk = self.chunk.append(data_slice)
            else:
                # return full chunk
                if not self.chunk_per_page:
                    yield self._get_chunk()

        # return remaining chunk
        if not self.chunk_per_page:
            yield self._get_chunk()

    def _get_chunk(self):
        if self.offset >= self.stop_dt:
            # terminate sequence
            return None
        # get chunk data based on time
        chunk_delta = self.channel._page_delta(self.chunk_size)
        end = self.offset + datetime.timedelta(microseconds=chunk_delta-1)
        chunk_data = self.chunk.loc[:end]
        # leave remainder
        start = end + datetime.timedelta(microseconds=1)
        self.chunk = self.chunk.loc[start:]
        self.offset = start
        if len(chunk_data):
            # valid data
            return chunk_data
        else:
            # empty chunk
            return pd.Series([])

    def __repr__(self):
        return "<ChannelIterator channel='{}' range=({},{})>".format(
                    self.channel.id, self.start, self.stop)


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Time Series API
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class TimeSeriesAPI(APIBase):
    base_uri = "/timeseries"
    name = 'timeseries'

    # ~~~~~~~~~~~~~~~~~~~
    # Channels
    # ~~~~~~~~~~~~~~~~~~~

    def create_channel(self, ts, channel):
        """
        Adds a channel to a timeseries package on the platform.
        """
        if channel.exists:
            return channel
        ts_id = self._get_id(ts)
        resp = self._post( self._uri('/{id}/channels', id=ts_id), json=channel.as_dict())

        ch = TimeSeriesChannel.from_dict(resp, api=self.session)
        ch._pkg = ts.id
        return ch

    def get_channels(self, ts):
        """
        Returns a set of channels for a timeseries package.
        """
        ts_id = self._get_id(ts)
        resp = self._get( self._uri('/{id}/channels', id=ts_id) )

        chs = [TimeSeriesChannel.from_dict(r, api=self.session) for r in resp]
        for ch in chs:
            ch._pkg = ts_id
        return chs

    def get_channel(self, pkg, channel):
        """
        Returns a channel object from the platform.
        """
        pkg_id = self._get_id(pkg)
        channel_id = self._get_id(channel)

        path = self._uri('/{pkg_id}/channels/{id}', pkg_id=pkg_id, id=channel_id)
        resp = self._get(path)

        ch = TimeSeriesChannel.from_dict(resp, api=self.session)
        ch._pkg = pkg_id
        return ch

    def update_channel(self, channel):
        """
        Updates a channel on the platform.

        Note: must be super-admin.
        """
        ch_id = self._get_id(channel)
        pkg_id = self._get_id(channel._pkg)
        path = self._uri('/{pkg_id}/channels/{id}', pkg_id=pkg_id, id=ch_id)

        resp = self._put(path , json=channel.as_dict())

        ch = TimeSeriesChannel.from_dict(resp, api=self.session)
        ch._pkg = pkg_id
        return ch

    def update_channel_properties(self, channel):
        """
        Updates a channel's properties on the platform.

        Note: must be super-admin.
        """
        ch_id = self._get_id(channel)
        pkg_id = self._get_id(channel._pkg)
        path = self._uri('/{pkg_id}/channels/{id}/properties', pkg_id=pkg_id, id=ch_id)

        resp = self._put(path , json=[m.as_dict() for m in channel.properties])

        ch = TimeSeriesChannel.from_dict(resp, api=self.session)
        ch._pkg = pkg_id
        return ch

    def delete_channel(self, channel):
        """
        Deletes a timeseries channel on the platform.
        """

        ch_id = self._get_id(channel)
        pkg_id = self._get_id(channel._pkg)
        path = self._uri('/{pkg_id}/channels/{id}', pkg_id=pkg_id, id=ch_id)

        return self._del(path)

    def delete_channel_by_id(self, pkg_id, channel_id):
        """
        Deletes a timeseries channel on the platform.
        """

        path = self._uri('/{pkg_id}/channels/{id}', pkg_id=pkg_id, id=channel_id)
        return self._del(path)

    def get_streaming_credentials(self, ts):
        """
        Get the streaming credentials for the given time series package.

        NOTE: this does not currently returns credentials but is still needed
        to make sure the package is registered in the system as a streaming
        time series package.
        """

        pkg_id = self._get_id(ts)

        return self._get(self._uri('/{pkg_id}/streaming_credentials', pkg_id=pkg_id))

    # ~~~~~~~~~~~~~~~~~~~
    # Data
    # ~~~~~~~~~~~~~~~~~~~

    def get_ts_data_iter(self, ts, start, end, channels, chunk_size,
                         use_cache,length=None):
        """
        Iterator will be constructed based over timespan (start,end) or (start, start+seconds)

        Both :chunk_size and :length should be described using strings, e.g.
          5 second  = '5s'
          3 minutes = '3m'
          1 hour    = '1h'
        otherwise microseconds assumed.
        """
        if isinstance(ts, basestring):
            # assumed to be package ID
            ts = self.session.core.get(ts)

        #CHANNELS
        ts_channels = ts.channels

        #no channels specified
        if channels is None:
            channels = ts.channels
        #1 channel specified as TSC object
        elif isinstance(channels,TimeSeriesChannel):
            channels = [channels]
        #1 channel specified and channel id
        elif isinstance(channels,basestring):
            channels = [ch for ch in ts.channels if ch.id==channels]
        #list of channel ids OR ts channels
        else:
            all_ch = []
            for chan in channels:
                if isinstance(chan,basestring):
                    all_ch.extend([ch for ch in ts_channels if ch.id==chan])
                else:
                    all_ch.extend([ch for ch in ts_channels if ch==chan])
            channels = all_ch

        # determine start (usecs)
        the_start = ts.start if start is None else infer_epoch(start)

        # chunk
        if chunk_size is not None and isinstance(chunk_size, basestring):
            chunk_size = parse_timedelta(chunk_size)

        # determine end
        if length is not None:
            if isinstance(length, basestring):
                length_usec = parse_timedelta(length)
            else:
                length_usec = length
            the_end = the_start + length_usec

        elif end is not None:
            the_end = infer_epoch(end)
        else:
            the_end = ts.end

        # logical check
        if the_end < the_start:
            raise Exception("End time cannot be before start time.")

        # loop through chunks
        the_start = long(the_start)
        the_end = long(the_end)

        channel_chunks = [
            ChannelIterator(ch, the_start, the_end, chunk_size,
                            api=self.session, use_cache=use_cache).get_chunks()
            for ch in channels
        ]

        while True:
            # get chunk for all channels
            values = [next(i, None) for i in channel_chunks]
            # no more results?
            if not [1 for v in values if v is not None]:
                break
            # make dataframe
            data_map = {c.name: v for c,v in zip(channels,values) if v is not None}
            yield pd.DataFrame.from_dict(data_map)

    def get_ts_data(self, ts, start, end, length, channels, use_cache):
        """
        Retrieve data. Must specify end-time or length.
        """
        ts_iter = self.get_ts_data_iter(ts=ts, start=start, end=end, channels=channels,
                                         chunk_size=None, use_cache=use_cache, length=length)
        df = pd.DataFrame()
        for tmp_df in ts_iter:
            df = df.append(tmp_df)
        return df

    def stream_data(self, ts, dataframe):
        """
        Stream timeseries data
        """
        stream = TimeSeriesStream(
            ts,
            self.session.settings.stream_name,
            self.session.settings.stream_max_segment_size,
            self.session.settings.stream_aws_region,
        )
        return stream.send_data(dataframe)

    def stream_channel_data(self, channel, series):
        """
        Stream channel data
        """
        raise NotImplementedError

    def get_segments(self, ts, channel, start, stop, gap_factor):
        """
        Retrieve ranges of time for channel (between start and stop)
        where there exists contiguous data. Gap detection sensitivity
        can be adjusted using ``gap_factor`` which is multiplied
        with sampling period of channel for identifying gaps.
        """
        channel_id = self._get_id(channel)
        package_id = self._get_id(ts)
        start = infer_epoch(start)
        stop  = infer_epoch(stop)

        resp = self._get(
            # Note: uses streaming server
            host     = self.session._streaming_host,
            endpoint = '/ts/retrieve/segments',
            base     = '',
            stream   = True,
            params   = dict(
                channel = channel_id,
                package = package_id,
                session = self.session.token,
                start   = start,
                end     = stop,
                gapThreshold = gap_factor
            )
        )
        return [tuple(x) for x in resp]

    # ~~~~~~~~~~~~~~~~~~~
    # Annotation Layers
    # ~~~~~~~~~~~~~~~~~~~

    def create_annotation_layer(self, ts, layer, description):

        if isinstance(layer,TimeSeriesAnnotationLayer):
            data = layer.as_dict()
        elif isinstance(layer,basestring):
            data = {
                'name' : layer,
                'description' : description
            }
        else:
            raise Exception("Layer must be TimeSeriesAnnotationLayer object or name of new layer")

        existing_layer = [i for i in ts.layers if data['name'] == i.name]
        if existing_layer:
            print 'Returning existing layer {}'.format(existing_layer)
            return existing_layer[0]
        else:
            ts_id = self._get_id(ts)
            path = self._uri('/{id}/layers', id=ts_id)
            resp = self._post(path, json=data)
            tmp_layer = TimeSeriesAnnotationLayer.from_dict(resp, api=self.session)
            if isinstance(layer,TimeSeriesAnnotationLayer):
                layer.__dict__.update(tmp_layer.__dict__)
            return tmp_layer

    def get_annotation_layer(self, ts, layer):
        ts_id = self._get_id(ts)
        layer_id = self._get_id(layer)
        path = self._uri('/{id}/layers/{layer_id}',id=ts_id,layer_id=str(layer_id))
        resp = self._get(path)
        return TimeSeriesAnnotationLayer.from_dict(resp, api=self.session)

    def get_annotation_layers(self, ts):
        ts_id = self._get_id(ts)
        resp = self._get(self._uri('/{id}/layers', id=ts_id))
        return [TimeSeriesAnnotationLayer.from_dict(x, api=self.session) for x in resp["results"]]

    def update_annotation_layer(self, ts, layer):
        #return all layers
        ts_id = self._get_id(ts)
        layer_id = self._get_id(layer)
        path = self._uri('/{id}/layers/{layer_id}', id=ts_id, layer_id=layer_id)
        resp = self._put(path, json=layer.as_dict())
        return TimeSeriesAnnotationLayer.from_dict(resp, api=self.session)

    def delete_annotation_layer(self, layer):
        ts_id = layer.time_series_id
        path = self._uri('/{id}/layers/{layer_id}',id = ts_id, layer_id =layer.id)
        try:
            self._del(path)
            layer.id = None
            return True
        except requests.exceptions.HTTPError:
            return False

    # ~~~~~~~~~~~~~~~~~~~
    # Annotations
    # ~~~~~~~~~~~~~~~~~~~

    def delete_annotation(self, annot):
        """
        Deletes a single annotation
        """
        path = self._uri('/{ts_id}/layers/{layer_id}/annotations/{annot_id}',
                    ts_id = annot.time_series_id,
                    layer_id = annot.layer_id,
                    annot_id = annot.id)
        try:
            self._del(path)
            annot.id = None
            return True
        except requests.exceptions.HTTPError:
            return False

    def create_annotations(self,layer, annotations):

        all_annotations = []

        if not isinstance(annotations,list):
            annotations = [annotations]

        for annot in annotations:
            tmp = self.create_annotation(layer=layer,annotation=annot)
            all_annotations.append(tmp)

        #if adding single annotation, return annotation object, else return list
        if len(all_annotations) == 1:
            all_annotations = all_annotations[0]

        return all_annotations

    def create_annotation(self, layer, annotation, **kwargs):
        """
        Creates annotation for some timeseries package on the platform.
        """
        if isinstance(annotation,TimeSeriesAnnotation):
            data = annotation.as_dict()
        elif all(x in kwargs for x in ['start','end']):
            start_time = infer_epoch(kwargs['start'])
            end_time = infer_epoch(kwargs['end'])
            data = {
                'name':'',
                'label':annotation,
                'start':long(start_time),
                'end':long(end_time),
            }
            if kwargs['channel_ids']:
                channel_ids = kwargs['channel_ids']
                if isinstance(channel_ids,basestring):
                    channel_ids = [channel_ids]
                data['channelIds']=channel_ids
            else:
                ts = layer._api.core.get(layer.time_series_id)
                data['channelIds']=[x.id for x in ts.channels]
            if 'description' in annotation:
                data['description']=kwargs['description']
            else:
                data['description']=None
        else:
            raise Exception("Must provide TimeSeriesAnnotation object or 'annotation','start','end' at minimum")

        data['time_series_id'] = layer.time_series_id
        data['layer_id'] = layer.id

        path = self._uri('/{ts_id}/layers/{layer_id}/annotations',
                    ts_id=layer.time_series_id, layer_id=layer.id)
        resp = self._post(path, json=data)
        tmp = TimeSeriesAnnotation.from_dict(resp, api=self.session)

        if isinstance(annotation,TimeSeriesAnnotation):
            annotation.__dict__.update(tmp.__dict__)

        return tmp

    def update_annotation(self, ts, layer, annot):
        """
        Update annotation on the platform.
        """
        path = self._uri('/{ts_id}/layers/{layer_id}/annotations/{annot_id}',
                    ts_id = self._get_id(ts),
                    layer_id = self._get_id(layer),
                    annot_id = self._get_id(annot))
        resp = self._put(path, json=annot.as_dict())
        return TimeSeriesAnnotation.from_dict(resp, api=self.session)

    def get_annotation(self, ts, layer, annot):
        """
        Returns a timeseries annotation
        """
        path = self._uri('/{ts_id}/layers/{layer_id}/annotations/{annot_id}',
                    ts_id = self._get_id(ts),
                    layer_id = self._get_id(layer),
                    annot_id = self._get_id(annot))
        resp = self._get(path)
        return TimeSeriesAnnotation.from_dict(resp["annotation"], api=self.session)

    def iter_annotations(self, ts, layer, window_size=10, channels=None):
        # window_size is seconds
        if not isinstance(ts, TimeSeries):
            raise Exception("Argument 'ts' must be TimeSeries.")

        if channels is None:
            # use all channels
            channels = ts.channels
        else:
            # make sure specified channels are in current timeseries
            ts_ch_ids = [ch.id for ch in ts.channels]
            for ch in channels:
                if ch in ts_channels:
                    continue
                raise Exception("Channel '{ch}' not found in TimeSeries '{ts}'".format(
                        ts = ts.id,
                        ch = self._get_id(ch)))

        # paginate annotations
        start_time, end_time = ts.limits()
        num_windows = (end_time-start_time)/(window_size*1e6)
        for i in range(int(np.ceil(num_windows))):
            win_start = start_time + i* (window_size*1e6)
            win_end = win_start + window_size*1e6
            if win_end > end_time:
                win_end = end_time
            annotations = self.query_annotations(ts=ts,layer=layer,start=win_start,end=win_end, channels=channels)
            yield annotations

    def get_annotations(self, ts, layer, channels=None):
        """
        Returns all annotations for a given layer
        """
        start, end = ts.limits()
        return self.query_annotations(ts=ts, layer=layer, start=start, end=end, channels=channels, limit=0, offset=0)


    def query_annotations(self, ts, layer, start=None, end=None, channels=None, limit=None, offset=0):
        """
        Retrieves timeseries annotations for a particular range  on array of channels.
        """
        if channels is None:
            ch_list = [] #empty uses all channels
        else:
            ch_list = [self._get_id(x) for x in channels]

        ts_start, ts_end = ts.limits()
        if start is None:
            start = ts_start
        elif isinstance(start, datetime.datetime):
            start = usecs_since_epoch(start)

        if end is None:
            end = ts_end
        elif isinstance(end, datetime.datetime):
            end = usecs_since_epoch(end)

        params = {
            'start': long(start),
            'end': long(end),
            'channelIds': ch_list,
            'layerName': layer.name,
            'limit': limit,
            'offset': offset
        }
        path = self._uri('/{ts_id}/layers/{layer_id}/annotations',
                    ts_id = ts.id,layer_id = layer.id)

        resp = self._get(path, params=params)

        return [TimeSeriesAnnotation.from_dict(x, api=self.session) for x in resp['annotations']['results']]

    def query_annotation_counts(self, ts, layers, start, end, period, channels=None, merge_periods=False):
        """
        Retrieves annotation counts for a given ts, channel, start, end, and/or layer

        Args:
            ts (TimeSeries)                  : The timeseries package for which to count annotations
            layers ([TimeSeriesLayer])       : List of layers for which to count annotations
            start (datetime or microseconds) : The starting time of the range to query
            end (datetime or microseconds)   : The ending time of the the range to query
            period (string)                  : The length of time to group the counts.
                                               Formatted as a string - e.g. '1s', '5m', '3h'
            channels ([TimeSeriesChannel])   : List of channel (if omitted, all channels will be used)
            merge_periods(Boolean)           : If true, merge consecutive result periods together to
                                               reduce the size of the resulting payload

        Returns:
            A dict
            layer_id -> list of counts for each period
        """
        if channels is None:
            ch_list = [] # empty uses all channels
        else:
            ch_list = [self._get_id(x) for x in channels]

        if isinstance(start, datetime.datetime):
            start = usecs_since_epoch(start)
        if isinstance(end, datetime.datetime):
            end = usecs_since_epoch(end)

        period = parse_timedelta(period)

        params = {
            'aggregation': 'count',
            'start': long(start),
            'end': long(end),
            'period': long(period),
            'mergePeriods': merge_periods,
            'layerIds': [l.id for l in layers],
            'channelIds': ch_list
        }

        path = self._uri('/{ts_id}/annotations/window',
                    ts_id = self._get_id(ts))

        resp = self._get(path, params=params)
        return resp

    def process_annotation_file(self,ts,file_path):
        """
        Processes the .bfannot file at file_path and adds to timeseries package
        """
        if not file_path.lower().endswith(('.bfannot')):
            raise Exception("Annotation file format not currently supported. Supported annotations types: .bfannot")
        try:
            df = pd.read_csv(file_path)
            df = df.where((pd.notnull(df)), None)
            channels = ts.channels
            if (df['version'][0] == 1.0): #version number
                layers = df['layer_name'].unique()
                for l in layers:
                    layer = []
                    annots = df.loc[df['layer_name'] == l]

                    #create or find existing layer
                    layer = ts.add_layer(layer=l, description=annots['layer_description'].iloc[0])

                    annotations = []
                    for index, row in annots.iterrows():
                        if pd.isnull(row['channel_names']):
                            channel_ids = [x.id for x in channels]
                        else:
                            channel_names = row['channel_names'].split(';')
                            channel_ids = [x.id for x in channels if x.name in channel_names]

                        layer.insert_annotation(annotation=row['annotation_label'],channel_ids=channel_ids,start=row['start_uutc'],end=row['end_uutc'],description=row['annotation_description'])

                    print 'Added annotations to layer {} , pkg: {}'.format(layer,ts)
            else:
                raise Exception('BF version {} not found or not supported'.format(df['version'][0]))

        except Exception, error:
            raise Exception("Error adding annotation file {}, {}".format(file_path, error))

    def write_annotation_file(self,ts,file_path,layer_names):
        """
        Writes all layers in ts to .bfannot (v1.0) file

        """
        layers = ts.layers
        if not layers:
            raise Exception('Timeseries has no existing layers')

        if not file_path.lower().endswith(('.bfannot')):
            file_path+='.bfannot'

        headers = ['version',
                    'package_type',
                    'layer_name',
                    'layer_description',
                    'annotation_label',
                    'start_uutc',
                    'end_uutc',
                    'channel_names',
                    'annotation_description']
        out = pd.DataFrame(columns=headers)

        if layer_names:
            if not isinstance(layer_names,list):
                layer_names = [layer_names]
            new_layers = [l for l in layers if l.name in layer_names]
            layers = new_layers

        to_write = []
        for l in layers:
            annot = l.annotations()
            for a in annot:
                channels = [ch.name for ch in ts.channels if ch.id in a.channel_ids]
                channel_names = ";".join(channels)
                tmp = {
                    'layer_name' : l.name,
                    'layer_description': l.description,
                    'annotation_label' : a.label,
                    'start_uutc' : long(a.start),
                    'end_uutc' : long(a.end),
                    'channel_names' : channel_names,
                    'annotation_description' : a.description
                }
                to_write.append(tmp)
        out = out.append(to_write)

        # Add version number and package type
        out.loc[0,('version')] = 1.0
        out.loc[0,('package_type')] = ts.type

        out.to_csv(file_path,index=False)

    # ~~~~~~~~~~~~~~~~~~~
    # Helpers
    # ~~~~~~~~~~~~~~~~~~~


    def _annotation_query_params(ts, start, end, period, layer, channels):
        # parse channel input
        channels = self._channel_list(ts, channels)

        # channel IDs
        ch_ids = [x.id for x in channels]

        params = {
            'start': usecs_since_epoch(start),
            'end': usecs_since_epoch(end),
            'channels': ch_ids,
            'period': period
        }

        if layer is not None:
            params['layer'] = layer
        return params

    def _channel_list(self, ts, channels):
        """
        Get list of channel objects provided flexible input values
        """
        ts_id = self._get_id(ts)

        if channels is None:
            # if channel(s) not specified, grab all for package
            channels = self.session.timeseries.get_channels(ts_id)

        # check if list
        if not hasattr(channels, '__iter__'):
            # they specified a single object
            channels = [channels]

        # check type of items in list
        for ch in channels:
            if isinstance(ch, TimeSeriesChannel):
                # Channel looks good
                continue
            if isinstance(ch, basestring):
                # Assume channel ID, get object
                ch = self.session.get(ch)
            else:
                raise Exception('Expecting TimeSeries instance or ID')

        return channels
