import pytest
import pdb
from blackfynn import TimeSeries, TimeSeriesChannel
from blackfynn.models import TimeSeriesAnnotationLayer, TimeSeriesAnnotation
import datetime
@pytest.fixture()
def timeseries(client, dataset):
    # create
    ts = TimeSeries('Human EEG')
    assert not ts.exists
    dataset.add(ts)
    assert ts.exists
    assert ts in dataset
    assert ts.type == 'TimeSeries'
    assert ts.name.startswith('Human EEG') #starts with bc duplicate names are appended
    ts2 = client.get(ts.id)
    assert ts2.id == ts.id
    assert ts2.name == ts.name
    assert ts2.type == 'TimeSeries'
    del ts2

    # provide to other tests
    yield ts

    # streaming credentials
    cred = ts.streaming_credentials()
    assert cred

    # remove
    dataset.remove(ts)
    assert not ts.exists
    assert ts not in dataset

def test_update_timeseries_name(client, timeseries):
    # update timeseries: change name
    timeseries.name = 'Monkey EEG'
    timeseries.update()
    timeseries2 = client.get(timeseries.id)
    assert timeseries2.id == timeseries.id
    assert timeseries2.name == timeseries.name
    assert timeseries2.type == 'TimeSeries'


def test_timeseries_channels(client, timeseries):
    num_channels = 10

    assert timeseries.exists

    ts2 = client.get(timeseries.id)

    chs = []
    for i in range(num_channels):
        # init
        chname = 'Channel-{}'.format(i)
        ch = TimeSeriesChannel(
                name=chname,
                rate=256,
                unit='uV')
        assert not ch.exists

        # create
        timeseries.add_channels(ch)
        assert ch.exists
        assert ch.name == chname
        chs.append(ch)

        # use separate request to get channel
        ch.insert_property('key','value')
        ch2 = filter(lambda x: x.id==ch.id, ts2.channels)[0]
        assert ch2.get_property('key') is not None
        assert ch2.name == ch.name
        assert ch2.type == ch.type
        assert ch2.rate == ch.rate
        assert ch2.unit == ch.unit
        del ch2

        # update channel
        ch.name = '{}-updated'.format(ch.name)
        ch.rate = 200
        ch.update()

        # use separate request to confirm name change
        ch2 = filter(lambda x: x.id==ch.id, ts2.channels)[0]
        assert ch2.name == ch.name
        assert ch2.rate == ch.rate

    # ensure correct number of channels
    channels = timeseries.channels
    assert len(channels) == num_channels

    # get channels (via API)
    ch_ids = [x.id for x in channels]

    # check 
    for ch in chs:
        assert ch.id in ch_ids

    # TODO: remove channel
    ch = channels[0]
    timeseries.remove_channels(ch)
    channels = timeseries.channels
    assert len(channels) == num_channels-1
    assert ch not in channels
    assert ch.id not in [x.id for x in channels]


def test_timeseries_annotations(client, timeseries):
    assert timeseries.exists
    print 'layers = ', timeseries.layers

    #Create Layer
    layer1 = TimeSeriesAnnotationLayer(name="test_layer", time_series_id = timeseries.id, description="test_description")
    a = layer1.as_dict()
    assert a['name'] == 'test_layer'
    assert a['description'] == 'test_description'

    # Add Layer
    timeseries.add_layer(layer1)
    assert layer1.exists

    # Get Layer
    layer1b = timeseries.get_layer('test_layer')
    assert layer1b.exists
    assert layer1b.name == "test_layer"
    assert layer1.id == layer1b.id
    assert layer1._api.timeseries is not None

    # Add another layer
    layer2 = timeseries.add_layer('test_layer2','test_description2')
    assert layer2.exists

    layer2copy = timeseries.add_layer('test_layer2')
    assert layer2copy.id == layer2.id

    # Get Layer
    layer2b = timeseries.get_layer('test_layer2')
    assert layer2b.exists
    assert layer2b.name == "test_layer2"
    assert layer2.id == layer2b.id
    assert layer2._api.timeseries is not None

    layers = timeseries.layers
    assert len(layers)==2
    assert layers[0].name == "test_layer"
    assert layers[1].name == 'test_layer2'

    #Create channels
    ch = TimeSeriesChannel(
        name='test_channel',
        rate=256,
        unit='uV',
        start=1,
        end=60*1e6)

    #Create annotation over one channel
    # create
    timeseries.add_channels(ch)
    assert ch.exists
    assert ch.id in [x.id for x in timeseries.channels]
    annot = TimeSeriesAnnotation(label = 'test_label', channel_ids = timeseries.channels[0].id,start = timeseries.channels[0].start,end=timeseries.channels[0].start+1*1e6)
    #Add Annotation
    layer1.add_annotations(annot)
    assert annot.exists

    # get annotations
    annotb = layer1.annotations()
    assert annotb[0].label == annot.label

    annotc = client._api.timeseries.get_annotation(timeseries, layer1, annot)
    assert annotc.label == annot.label

    #Create annotation over multiple channels
    ch2 = TimeSeriesChannel(
        name='test_channel',
        rate=256,
        unit='uV',
        start=1,
        end=60*1e6)

    timeseries.add_channels(ch2)
    channels = timeseries.channels
    ch_ids = [x.id for x in channels]
    assert ch2.exists
    assert ch2.id in ch_ids
    assert ch.id in ch_ids
    for ch in channels:
        assert ch.rate == 256
        assert ch.exists

    #add annotation over two channels
    channel_ids = [timeseries.channels[x].id for x in range(len(timeseries.channels))]

    annot2 = layer1.insert_annotation(annotation='test_label2', channel_ids = channel_ids,
        start =timeseries.channels[0].start+1*1e6,
        end = timeseries.channels[0].start+2*1e6)


    assert annot2.exists

    annot_gen = layer1.iter_annotations(1)
    annot = annot_gen.next()
    assert annot[0].label == 'test_label'
    next_annot= annot_gen.next()
    assert next_annot[0].label == 'test_label2'


    ### TEST DELETION

    annot3 = TimeSeriesAnnotation(label= 'test_label3', channel_ids= channel_ids,start = timeseries.channels[0].start+1*1e6,end=timeseries.channels[0].start+2*1e6)
    #layer1.add_annotations([annot2,annot3])
    annot3 = timeseries.add_annotations(layer=layer1,annotations=annot3)
    assert annot3.exists
    annot3.delete()
    assert not annot3.exists

    annot4 = timeseries.insert_annotation(layer=layer1,annotation='test_label3',start= timeseries.channels[0].start+1*1e6,end=timeseries.channels[0].start+2*1e6)
    assert annot4.exists
    annot4.delete()
    assert not annot4.exists

    annot5 = timeseries.insert_annotation(layer='test_layer4',annotation='test_label3',start=timeseries.channels[0].start+1*1e6,end=timeseries.channels[0].start+2*1e6)
    assert annot5.exists
    annot5.delete()
    assert not annot5.exists

    layer1.add_annotations([annot2,annot3])
    assert annot2.exists
    assert annot3.exists

    #test datetime input
    annot4 = timeseries.insert_annotation(layer='test_layer4',annotation='test_label3',start= datetime.datetime.utcfromtimestamp((timeseries.channels[0].start+1*1e6)/1e6),end=datetime.datetime.utcfromtimestamp(timeseries.channels[0].start+2*1e6))
    assert annot4.exists
    annot4.delete()
    assert not annot4.exists

    layer = timeseries.get_layer('test_layer4')
    assert layer.exists
    layer.delete()
    assert not layer.exists


    # delete annotations
    annot[0].delete()
    assert not annot[0].exists

    assert timeseries.exists
    timeseries.delete_layer(layer1)
    assert not layer1.exists

    assert layer2.exists
    layer2.delete()
    assert not layer2.exists

