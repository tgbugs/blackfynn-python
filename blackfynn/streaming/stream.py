import time
import boto3
import base64
import random
import hashlib
import datetime
import numpy as np
import pandas as pd

# blackfynn
from blackfynn import settings
from blackfynn.streaming.segment_pb2 import IngestSegment
from blackfynn.utils import usecs_since_epoch

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Time Series Stream (upload)
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class TimeSeriesStream():

    def __init__(self, ts):
        self.name = settings.stream_name
        self.max_segment_size = settings.stream_max_segment_size

        self.conn = boto3.client('kinesis', region_name=settings.stream_aws_region)

        # reference time-series
        self.ts = ts
        # cache channels
        self._channels = ts.channels

        self.registered = False
    
    @property
    def status(self):
        r = conn.describe_stream(self.name)
        description = r.get('StreamDescription')
        return description.get('StreamStatus')

    def _channel_by_name(self, name):
        matches = filter(lambda c: c.name==name, self._channels)
        if len(matches) > 1:
            raise Exception("Too many channels match name '{}'".format(name))
        if len(matches) == 0:
            raise Exception("No channels match name '{}'".format(name))
        return matches[0]

    def _channel_by_id(self, id):
        matches = filter(lambda c: c.id==id, self._channels)
        if len(matches) == 0:
            raise Exception("No channels match ID '{}'".format(id))
        return matches[0]

    def wait_until_ready(self, timeout=120):
        """
        Waits until Kinesis stream is ACTIVE
        """
        timeout = 20
        start = datetime.datetime.now()
        while self.status != 'ACTIVE':
            # sleep for a second
            time.sleep(1.5) 
            now = datetime.datetime.now()
            if (now-start).total_seconds() > timeout:
                raise Exception("Timeout waiting for stream connection.")

    def _make_ingest_segment(self, start_time,channel_id,period,values):
        segment = IngestSegment()
        if isinstance(start_time, datetime.datetime):
            segment.startTime = usecs_since_epoch(start_time)
        else:
            segment.startTime = start_time
        segment.channelId = channel_id
        segment.samplePeriod = period
        segment.data.extend(values)
        return segment

    def _send_segment(self, seg):
        """
        Send a single segment (protobuf) of data to streaming server
        """
        msg = base64.b64encode(seg.SerializeToString())
        pkey = hashlib.sha256(msg).hexdigest()
        resp = self.conn.put_record(
            StreamName=self.name,
            Data=msg,
            PartitionKey=pkey
        )
        return resp

    def _send_contiguous(self, channel, data, period):
        """
        Provided channel series values, send segments in chunks

        channel:  Blackfynn TimeSeriesChannel
        data:     Pandas Series
        period:   Sample period of data

        """
        for offset in np.arange(0, len(data), self.max_segment_size):
            # divide data into chunks
            chunk = data[offset:offset+self.max_segment_size]
            # make data segment
            seg = self._make_ingest_segment(
                        start_time=chunk.index[0],
                        channel_id=channel.id,
                        period=period,
                        values=chunk.values)
            # send data segment
            resp = self._send_segment(seg)

            if 'ResponseMetadata' not in resp:
                raise Exception("Incorrect response from Kinesis.")
            if resp['ResponseMetadata']['HTTPStatusCode'] != 200:
                raise Exception("Received non-200 response code from Kinesis.")

        # NOTE: this should be done by the streaming consumer (server),
        #       but until then, we'll update channel start/end time
        start = usecs_since_epoch(data.index[0])
        end = usecs_since_epoch(data.index[-1])
        if end > channel.end:
            channel.end = end
        if start < channel.start or channel.start == 0:
            channel.start = start
        channel.update()

    def send_data(self, df):
        """
        Provided a Pandas DataFrame, send streaming data to server. Data 
        is streamed in contiguous chunks. 

        1) Channels *must* already exist on platform.

        2) For each channel, the sample rate *must* match the rate of 
           the channel on the platform. 

        3) df.columns should reflect channels of timeseries packages, where
           df.values are data points. Columns can be channel names or IDs.

        """

        # sanity checks
        if not isinstance(df, pd.DataFrame):
            raise Exception("argument df must be Pandas DataFrame")

        # iterate over channels in data
        for col in df.columns:
            try:
                # match column to channel name
                channel = self._channel_by_name(col)
            except:
                # match column to channel ID
                channel = self._channel_by_id(col)

            self.send_channel_data(channel=channel, series=df[col])

    def send_channel_data(self, channel, series):

        if not isinstance(series, pd.Series):
            raise Exception("Series must be a Pandas Series object.")

        if self.registered == False:
            self.ts.streaming_credentials()
            self.registered = True

        # period in microseconds
        period = 1e6/channel.rate 

        # find timestamp differences
        ind = series.index.values
        ts_steps = (ind[1:] - ind[:-1]).astype('timedelta64[us]').astype(int)

        # find gaps
        gap_fuzz = period*0.75
        gaps = ts_steps > period + gap_fuzz
        gaps_ind = np.where(gaps)[0]
        gap_pairs = zip(np.hstack((0,gaps_ind)), np.hstack((gaps_ind, len(series))))

        # iterate over contiguous segments
        for starti, endi in gap_pairs:
            if (endi-starti) < 10:
                raise Exception("Data contains extremely small contiguous section {}[{}:{}]" \
                                    .format(channel.id,starti,endi))

            print "Sending contiguous data: {} to {}".format(starti, endi)
            # get contiguous segment
            data = series[starti:endi+1]
            # send contiguous region of data
            self._send_contiguous(channel=channel, data=data, period=period)
