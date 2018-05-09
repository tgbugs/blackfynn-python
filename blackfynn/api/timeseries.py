# -*- coding: utf-8 -*-

import re
import math
import datetime
import numpy as np
import pandas as pd
import json
from types import NoneType
from itertools import islice, count
from concurrent.futures import ThreadPoolExecutor

# blackfynn
from blackfynn.api.base import APIBase
from blackfynn.streaming import TimeSeriesStream
from blackfynn.utils import (
    usecs_to_datetime, usecs_since_epoch, infer_epoch, log
)
from blackfynn.models import (
    File, TimeSeries,TimeSeriesChannel, TimeSeriesAnnotation,
    get_package_class, TimeSeriesAnnotation, TimeSeriesAnnotationLayer
)
from blackfynn.generated.timeseries_pb2 import AgentTimeSeriesResponse

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

def parse_start_end(ts, start, end, length):
    # determine start (usecs)
    the_start = ts.start if start is None else infer_epoch(start)

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

    return the_start, the_end

vec_usecs_to_datetime = np.vectorize(usecs_to_datetime)

class AgentException(Exception):
    pass

class AgentTimeSeriesSocket(object):
    def __init__(
        self,
        api,
        package,
        channels,
        start,
        end,
        chunk_size,
        use_cache=True
    ):
        self.new_command = {
            "command":          "new",
            "session":          api.token,
            "packageId":        package,
            "channels":         [{"id": c.id, "rate": c.rate} for c in channels],
            "startTime":        start,
            "endTime":          end,
            "chunkSize":        chunk_size,
            "useCache":         use_cache,
        }
        self.ws = api.create_agent_socket(
            "ts/query?session={}&package={}".format(
                api.token,
                package
            )
        )
        self.channels = { c.id: c for c in channels }

    def __iter__(self):
        status = "READY"
        self.ws.send(json.dumps(self.new_command))
        while status == "READY":
            result = self.ws.recv()
            response = AgentTimeSeriesResponse.FromString(result)
            response_type = response.WhichOneof("response_oneof")
            if response_type == "state":
                status = response.state.status
                if status == "ERROR":
                    raise AgentException(response.state.description)
            elif response_type == "chunk":
                yield self.parse_chunk(response.chunk)
            else:
                raise AgentException("Received unknown data from agent")
            if status == "READY":
                self.ws.send(json.dumps({"command": "next"}))
        self.ws.send(json.dumps({"command": "close"}))

    def parse_chunk(self, chunk):
        return { self.channels[channel.id].name: {
            usecs_to_datetime(datum.time): datum.value for datum in channel.data
        } for channel in chunk.channels }

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
                         use_cache, length=None):
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


        # chunk
        if chunk_size is None:
            chunk_size = 100
        if chunk_size is not None and isinstance(chunk_size, basestring):
            chunk_size = parse_timedelta(chunk_size)

        the_start, the_end = parse_start_end(ts, start, end, length)

        frames = AgentTimeSeriesSocket(
            self.session,
            ts.id,
            channels,
            the_start,
            the_end,
            chunk_size,
            use_cache,
        )

        channel_name_map = { c.id: c.name for c in channels }

        for frame in frames:
            yield pd.DataFrame.from_dict(frame)

    def get_ts_data(self, ts, start, end, length, channels, use_cache):
        """
        Retrieve data. Must specify end-time or length.
        """
        the_start, the_end = parse_start_end(ts, start, end, length)

        chunk_size = the_end - the_start

        ts_iter = self.get_ts_data_iter(ts=ts, start=start, end=end, channels=channels,
                                         chunk_size=chunk_size, use_cache=use_cache, length=length)
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

        path = self._uri('/{id}/layers/{layer_id}',id = ts_id,  layer_id =layer.id)
        if self._del(path):
            layer.id = None
            return True
        else:
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
        if self._del(path):
            annot.id = None
            return True
        else:
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

    def annotation_counts(self, ts, layer, start, end, period, channels=None):
        """
        Retrives annotation counts for a given ts, channel, start, end, and/or layer
        """
        if channels is None:
            ch_list = [] #empty uses all channels
        else:
            ch_list = [self._get_id(x) for x in channels]

        if isinstance(start, datetime.datetime):
            start = usecs_since_epoch(start)
        if isinstance(end, datetime.datetime):
            end = usecs_since_epoch(end)

        period = parse_timedelta(period)

        params = {
            'start': long(start),
            'end': long(end),
            'channelIds': ch_list,
            'period': period,
            'layer': layer.name
        }

        path = self._uri('/{ts_id}/annotations',
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


