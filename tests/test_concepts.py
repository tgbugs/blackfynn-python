import datetime
from collections import namedtuple
from past.builtins import unicode  # Alias of str in Python 3

import pytest

from blackfynn.models import (
    DataPackage,
    ModelProperty,
    ModelPropertyEnumType,
    ModelPropertyType,
    convert_datatype_to_type,
    convert_type_to_datatype,
    uncast_value
)
from tests.utils import create_test_dataset, current_ts, get_test_client


@pytest.mark.parametrize('fromtype,datatype,totype', [
    (ModelPropertyType(data_type=int), 'long', int),
    (int, 'long', int),
    (ModelPropertyType(data_type=unicode), 'string', unicode),
    (datetime.date, 'date', datetime.datetime),
    ('date', 'date', datetime.datetime),
    (ModelPropertyType(data_type=str, format='date'), 'string', unicode),
])
def test_model_type_conversion(fromtype, datatype, totype):
    assert convert_type_to_datatype(fromtype) == datatype
    assert  convert_datatype_to_type(datatype) == totype


@pytest.mark.parametrize('json,data_type,format,unit', [
    ('string', unicode, None, None),
    ({'type':'string', 'format':'date'}, unicode, 'date', None),
    ({'type': 'double', 'unit': 'kg'}, float, None, 'kg'),
])
def test_model_type_from_dict(json, data_type, format, unit):
    decoded = ModelPropertyType.from_dict(json)
    assert isinstance(decoded, ModelPropertyType)
    assert decoded.data_type == data_type
    assert decoded.format == format
    assert decoded.unit == unit


def test_model_with_invalid_properties(dataset):
    invalid_schema = [('an_integer', int, 'An Integer')]
    new_model = dataset.create_model(
        'New_Model_{}'.format(current_ts()), 'A New Model', 'a new model', invalid_schema
    )

    assert not new_model.schema


def test_date_formatting():
    d1 = datetime.datetime(2018, 8, 24, 15, 11, 25)
    assert uncast_value(d1) == '2018-08-24T15:11:25.000000+00:00'

    d2 = datetime.datetime(2018, 8, 24, 15, 11, 25, 1)
    assert uncast_value(d2) == '2018-08-24T15:11:25.000001+00:00'


def test_models(dataset):
    schema = [('an_integer', int, 'An Integer', True), ('a_bool', bool), ('a_string', str), ('a_datetime', datetime.datetime)]
    display_name = 'A New Property'
    description = 'a new description'
    values = {'an_integer': 100, 'a_bool': True, 'a_string': 'fnsdlkn#$#42nlfds$3nlds$#@$23fdsnfkls', 'a_datetime': datetime.datetime.now()}

    #################################
    ## Models
    ################################

    models = dataset.models()

    new_model = dataset.create_model('New_Model_{}'.format(current_ts()), 'A New Model', 'a new model', schema)

    assert len(dataset.models()) == len(models) + 1

    assert dataset.get_model(new_model.id) == new_model
    assert dataset.get_model(new_model.type) == new_model

    # Check that local changes get propagated
    new_model.add_property('a_new_property', str, display_name)
    new_model.description = description
    new_model.update()
    new_model = dataset.get_model(new_model.id)
    assert new_model.description == description
    assert new_model.get_property('a_new_property').display_name == display_name

    new_model.add_properties([('a_new_float', float), {'name': 'a_new_int', 'data_type': int}, 'a_new_string'])
    assert new_model.get_property('a_new_float').type == float
    assert new_model.get_property('a_new_int').type == int
    assert new_model.get_property('a_new_string').type == unicode

    nc_one = new_model.create_record(values)
    nc_two = new_model.create_record({'an_integer': 1, 'a_bool': False, 'a_string': '', 'a_datetime': datetime.datetime.now()})
    nc_three = new_model.create_record({'an_integer': 10000, 'a_bool': False, 'a_string': '43132312', 'a_datetime': datetime.datetime.now()})
    nc_four = new_model.create_record({'a_datetime': datetime.datetime.now()})

    nc_delete_one = new_model.create_record({'a_datetime': datetime.datetime.now()})
    nc_delete_two = new_model.create_record({'a_datetime': datetime.datetime.now()})

    with pytest.raises(Exception):
        new_model.create_record()

    new_models_old = new_model.get_all()
    assert new_model.get_all(limit=1) == new_models_old[:1]
    assert new_model.get_all(limit=2, offset=2) == new_models_old[2:4]

    new_model.delete_records(nc_delete_one, nc_delete_two.id)
    new_models = new_model.get_all()

    assert len(new_models) == (len(new_models_old) - 2)

    assert nc_two.model == new_model
    assert nc_two.get('a_string') == ''

    nc_four.set('a_string', 'hello')
    assert nc_four.get('a_string') == new_model.get(nc_four).get('a_string')

    with pytest.raises(Exception):
        nc_four.set('an_integer', datetime.datetime.now())

    assert nc_four.get('an_integer') == None
    nc_four.set('an_integer', 10)
    assert nc_four.get('an_integer') == 10

    nc_delete_three = new_model.create_record({'a_string': 'delete me'})
    assert len(new_model.get_all()) == len(new_models) + 1
    nc_delete_three.delete()
    assert len(new_model.get_all()) == len(new_models)

    # cannot add a record id column using an existing name
    with pytest.raises(ValueError):
        new_model.get_all().as_dataframe(
            record_id_column_name=list(new_model.get_all().type.schema.keys())[0]
        )

    # assert no extra columns are added by default
    df_cs_no_rec = new_model.get_all().as_dataframe()
    assert len(df_cs_no_rec.columns) == len(new_model.get_all().type.schema.keys())

    # assert record id column is added when arg is present and valid
    df_cs = new_model.get_all().as_dataframe(
        record_id_column_name='record_id'
    )

    # confirm that all record ids are present in this dataframe
    assert 'record_id' in df_cs.columns
    for record in new_model.get_all():
        assert not df_cs.query('record_id == @record.id').empty

    #################################
    ## Relationships
    ################################

    relationships = dataset.relationships()

    new_relationship = dataset.create_relationship_type('New_Relationship_{}'.format(
        current_ts()), 'a new relationship')

    assert len(dataset.relationships()) == len(relationships) + 1

    assert dataset.get_relationship(new_relationship.id) == new_relationship
    assert dataset.get_relationship(new_relationship.type) == new_relationship

    nr_one = new_relationship.relate(nc_one, nc_two)
    nr_four = new_relationship.relate(nc_four, nc_one)
    nr_five = new_relationship.relate(nc_four, nc_two)

    nr_two = nc_two.relate_to(nc_three, new_relationship)
    nr_three = nc_three.relate_to(nc_four, new_relationship)
    nr_six = nc_four.relate_to(nc_three, new_relationship)

    assert len(nc_four.get_related(new_model.type)) == 4

    new_relationships = new_relationship.get_all()

    nr_delete_three = new_relationship.relate(nc_one, nc_two)
    assert len(new_relationship.get_all()) == len(new_relationships) + 1
    nr_delete_three.delete()
    assert len(new_relationship.get_all()) == len(new_relationships)

    df_rs = new_relationship.get_all().as_dataframe()

    p = DataPackage('test-csv', package_type='Tabular')
    dataset.add(p)
    dataset.update()
    assert p.exists

    p.relate_to(nc_one)
    p.relate_to(nc_two)
    nc_three.relate_to(p, new_relationship)
    new_relationship.relate(nc_four, p)

    assert len(nc_four.get_related(new_model.type)) == 4

def test_simple_model_properties(dataset):

     # Define properties as tuples
     model_with_basic_props_1 = dataset.create_model('Basic_Props_1', description='a new description',schema=
     [('an_integer', int, 'An Integer', True), ('a_bool', bool), ('a_string', str), ('a_date', datetime.datetime)])

     assert dataset.get_model(model_with_basic_props_1.id) == model_with_basic_props_1

     # Add a property with a description
     model_with_basic_props_1.add_property('a_new_property', float, display_name='Weight', description="some metric")

     updated_model_1 = dataset.get_model(model_with_basic_props_1.id)

     test_prop = updated_model_1.get_property('a_new_property')
     assert test_prop.display_name == 'Weight'
     assert test_prop.description == "some metric"

     # Define properties as ModelProperty objects
     model_with_basic_props_2 = dataset.create_model('Basic_Props_2', description='a new description', schema=[
         ModelProperty('name', data_type=str, title=True, required=True),
         ModelProperty('age', data_type=int),
         ModelProperty('DOB', data_type=datetime.datetime)
     ])

     assert dataset.get_model(model_with_basic_props_2.id) == model_with_basic_props_2

     # Add a property
     model_with_basic_props_2.add_property('weight2', ModelPropertyType(data_type=float), display_name='Weight')

     updated_model_2 = dataset.get_model(model_with_basic_props_2.id)

     assert updated_model_2.get_property('weight2').display_name == 'Weight'
     assert updated_model_2.get_property('name').required == True

     # Define properties as ModelProperty objects with ModelPropertyType data_type
     model_with_basic_props_3 = dataset.create_model('Basic_Props_3', description='a new description', schema=[
         ModelProperty('name', data_type=ModelPropertyType(data_type=str), title=True),
         ModelProperty('age', data_type=ModelPropertyType(data_type=int)),
         ModelProperty('DOB', data_type=ModelPropertyType(data_type=str))
     ])

     assert dataset.get_model(model_with_basic_props_3.id) == model_with_basic_props_3

     # Add a property
     model_with_basic_props_3.add_property('weight3', ModelPropertyType(data_type=float), display_name='Weight')

     updated_model_3 = dataset.get_model(model_with_basic_props_3.id)

     assert updated_model_3.get_property('weight3').display_name == 'Weight'

     # Reverse look up property data types
     model_with_basic_props_4 = dataset.create_model('Basic_Props_4', description='a new description', schema=[
         ModelProperty('name', data_type='string', title=True, required=True),
         ModelProperty('age', data_type='long'),
         ModelProperty('DOB', data_type='date')
     ])

     assert dataset.get_model(model_with_basic_props_4.id) == model_with_basic_props_4

     # Add a property
     model_with_basic_props_4.add_property('weight4', ModelPropertyType(data_type='double'), display_name='Weight')

     updated_model_4 = dataset.get_model(model_with_basic_props_4.id)

     assert updated_model_4.get_property('weight4').display_name == 'Weight'
     assert updated_model_4.get_property('name').required == True

def test_complex_model_properties(dataset):
    model_with_complex_props = dataset.create_model('Complex_Props', description='a new description', schema=[
        ModelProperty('name', data_type=ModelPropertyType(data_type=str), title=True),
        ModelProperty('age', data_type=ModelPropertyType(data_type=int)),
        ModelProperty('email', data_type=ModelPropertyType(data_type=str, format='email'))
    ])

    assert dataset.get_model(model_with_complex_props.id) == model_with_complex_props

    # Add a property
    model_with_complex_props.add_property('weight', ModelPropertyType(data_type=float, unit='kg'), display_name='Weight')

    updated_model = dataset.get_model(model_with_complex_props.id)

    weight_property = updated_model.get_property('weight')
    assert (weight_property.display_name == 'Weight')
    assert (weight_property.type == float)
    assert (weight_property._type.data_type == float)
    assert (weight_property._type.format == None)
    assert (weight_property.unit == 'kg')

    email_property = model_with_complex_props.get_property('email')
    assert (email_property.type == unicode)
    assert (email_property._type.data_type == unicode)
    assert (email_property._type.format.lower() == 'email')
    assert (email_property.unit == None)

    good_values = {'name': 'Bob', 'age': 1, 'email': 'test@test.com', 'weight': 10}
    good_record = updated_model.create_record(good_values)

    bad_values = {'name': 'Bob', 'age': 1, 'email': '123455', 'weight': 10}

    with pytest.raises(Exception):
        bad_record = updated_model.create_record(bad_values)

def test_model_properties_with_enum(dataset):
    model_with_enum_props = dataset.create_model('Enum_Props', description='a new description', schema=[
        ModelProperty('name', data_type=str, title=True),
        ModelProperty('some_enum', data_type=ModelPropertyEnumType(data_type=float, enum=[1.0, 2.0, 3.0], unit="cm", multi_select=False)),
        ModelProperty('some_array',
                      data_type=ModelPropertyEnumType(data_type=str, enum=['foo', 'bar', 'baz'], multi_select=True)),
        ModelProperty('non_enum_array', data_type=ModelPropertyEnumType(data_type=int, multi_select=True))
    ])

    result = dataset.get_model(model_with_enum_props.id)

    assert result == model_with_enum_props

    enum_property = result.get_property('some_enum')
    array_property = result.get_property('some_array')
    non_enum_array_property = result.get_property('non_enum_array')

    assert (enum_property.type == float)
    assert (enum_property.multi_select == False)
    assert (enum_property.enum == [1.0, 2.0, 3.0])
    assert (enum_property.unit == 'cm')

    assert (array_property.type == unicode)
    assert (array_property.multi_select == True)
    assert (array_property.enum == ['foo', 'bar', 'baz'])

    assert non_enum_array_property.type == int
    assert non_enum_array_property.multi_select == True
    assert non_enum_array_property.enum == None


Graph = namedtuple('TestGraph', ['dataset', 'models', 'model_records',
                                 'relationships', 'relationship_records'])


@pytest.fixture(scope="module")
def simple_graph(client):
    """
        Creates a small test graph in an independent dataset to de-couple
        from other tests
    """
    test_dataset = create_test_dataset(client)
    model_1 = test_dataset.create_model(
        'Model_A', description="model a", schema=[
            ModelProperty("prop1", data_type=ModelPropertyType(data_type=str),
                          title=True)])

    model_2 = test_dataset.create_model(
        'Model_B', description="model b",
        schema=[ModelProperty("prop1",
                              data_type=ModelPropertyType(data_type=str),
                              title=True)
                ])

    relationship = test_dataset.create_relationship_type(
        'New_Relationship_{}'.format(current_ts()), 'a new relationship')

    model_instance_1 = model_1.create_record({"prop1": "val1"})
    model_instance_2 = model_2.create_record({"prop1": "val1"})
    model_instance_1.relate_to(model_instance_2, relationship)

    graph = Graph(test_dataset, models=[model_1, model_2],
                  model_records=[model_instance_1, model_instance_2],
                  relationships=[relationship],
                  relationship_records=None)
    yield graph

    ds_id = test_dataset.id
    client._api.datasets.delete(test_dataset)
    all_dataset_ids = [x.id for x in client.datasets()]
    assert ds_id not in all_dataset_ids
    assert not test_dataset.exists


def test_get_related_models(simple_graph):
    related_models = simple_graph.models[0].get_related()
    assert len(related_models) == 1
    assert related_models[0].type == simple_graph.models[1].type


def test_related_records_pagination(dataset):
    patient = dataset.create_model(
        'patient', description="patient", schema=[
            ModelProperty("name", data_type=ModelPropertyType(data_type=str),
                          title=True)])

    visit = dataset.create_model(
        'visit', description="visit", schema=[
            ModelProperty("field", data_type=ModelPropertyType(data_type=str),
                          title=True)])

    attends = dataset.create_relationship_type('attends', 'an attendance')

    patient1 = patient.create_record({"name": "Fred"})
    visits = visit.create_records([{"field": str(i)} for i in range(200)])
    patient1.relate_to(visits, attends)

    # Get all records
    gotten = patient1.get_related()
    assert len(gotten) == 200
    assert [r.get("field") for r in gotten] == list(map(str, range(200)))


def test_get_topology(simple_graph):
    topology = simple_graph.dataset.get_topology()
    assert 'models' in topology
    assert len(topology['models']) == 2
    assert 'relationships' in topology
    assert len(topology['relationships']) == 1
