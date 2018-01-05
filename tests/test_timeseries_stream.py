
import time
import datetime

# blackfynn
from blackfynn import Blackfynn, TimeSeries, TimeSeriesChannel
from blackfynn.streaming import TimeSeriesStream
from blackfynn.utils import generate_dataframe


def test_stream_upload(use_dev, client, dataset):
    # can only run against dev server :-(
    if not use_dev: return

    # generate data
    freq = 100
    df = generate_dataframe(minutes=1, freq=100)

    # create timeseries
    ts = TimeSeries('My Test TimeSeries')
    dataset.add(ts)
    assert ts.exists
    print ts

    # create channels
    channels = [TimeSeriesChannel(c, rate=freq) for c in df.columns]
    ts.add_channels(*channels)
    for ch in channels:
        assert ch.exists
        assert ch.rate == freq
        assert ch.name in df.columns
    print "channels =", ts.channels

    # stream data up
    ts.stream_data(df)

    # wait a bit -- let server write data
    time.sleep(10)

    # retrieve data
    for chunk in ts.get_data_iter():
        assert len(chunk) > 0
        assert chunk.columns == df.columns
    assert chunk.index[-1] == df.index[-1]

    # make sure channel times were updated
    for ch in ts.channels:
        assert ch.start_datetime == df.index[0]
        assert ch.end_datetime == df.index[-1]
