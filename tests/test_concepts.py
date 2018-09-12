import time
import pytest
import datetime

from blackfynn.models import DataPackage, ModelPropertyType, ModelPropertyEnumType, ModelProperty, convert_type_to_datatype, convert_datatype_to_type, uncast_value

def current_ts():
    return int(round(time.time() * 1000))

def test_parse_model_datatype(dataset):
    basic_numeric_1 = ModelPropertyType(data_type=long)
    basic_numeric_2 = long

    basic_string_1 = ModelPropertyType(data_type=unicode)
    basic_string_2 = 'date'
    complex_string = ModelPropertyType(data_type=str, format='date')

    numeric_datatype_1 = convert_type_to_datatype(basic_numeric_1)
    numeric_datatype_2 = convert_type_to_datatype(basic_numeric_2)
    string_datatype_1 = convert_type_to_datatype(basic_string_1)
    string_datatype_2 = convert_type_to_datatype(basic_string_2)
    complex_datatype = convert_type_to_datatype(complex_string)

    assert numeric_datatype_1 == 'long'
    assert numeric_datatype_2 == 'long'
    assert string_datatype_1 == 'string'
    assert string_datatype_2 == 'date'
    assert complex_datatype == 'string'

    basic_type_1 = convert_datatype_to_type(string_datatype_1)
    assert basic_type_1 == unicode

    basic_type_2 = convert_datatype_to_type(string_datatype_2)
    assert basic_type_2 == datetime.datetime

    simple_json = 'string'
    decoded_simple = ModelPropertyType.from_dict(simple_json)
    assert (isinstance(decoded_simple, ModelPropertyType))
    assert (decoded_simple.data_type == unicode)
    assert (decoded_simple.format == None)
    assert (decoded_simple.unit == None)

    nested_json_1 = {'type':'string', 'format':'date'}
    decoded_string = ModelPropertyType.from_dict(nested_json_1)
    assert(isinstance(decoded_string, ModelPropertyType))
    assert(decoded_string.data_type == unicode)
    assert(decoded_string.format == 'date')
    assert(decoded_string.unit == None)

    nested_json_2 = {'type': 'double', 'unit': 'kg'}
    decoded_numeric = ModelPropertyType.from_dict(nested_json_2)
    assert (isinstance( decoded_numeric, ModelPropertyType))
    assert (decoded_numeric.data_type == float)
    assert (decoded_numeric.unit == 'kg')
    assert (decoded_numeric.format == None)

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
    schema = [('an_integer', int, 'An Integer', True), ('a_long', long), ('a_bool', bool), ('a_string', str), ('a_datetime', datetime.datetime)]
    display_name = 'A New Property'
    description = 'a new description'
    values = {'an_integer': 100, 'a_long': 100000L, 'a_bool': True, 'a_string': 'fnsdlkn#$#42nlfds$3nlds$#@$23fdsnfkls', 'a_datetime': datetime.datetime.now()}

    #################################
    ## Models
    ################################

    models = dataset.models()

    new_model = dataset.create_model('New_Model_{}'.format(current_ts()), 'A New Model', 'a new model', schema)

    assert len(dataset.models()) == len(models) + 1

    assert dataset.get_model(new_model.id) == new_model
    assert dataset.get_model(new_model.type) == new_model

    new_model.add_property('a_new_property', str, display_name)
    new_model.description = description
    new_model.update()

    new_model = dataset.get_model(new_model.id)

    assert new_model.description == description
    assert new_model.get_property('a_new_property').display_name == display_name

    new_model.add_properties([('a_new_float', float), {'name': 'a_new_int', 'data_type': int}, 'a_new_string'])
    assert new_model.get_property('a_new_float').type == float
    assert new_model.get_property('a_new_int').type == long
    assert new_model.get_property('a_new_string').type == unicode

    nc_one = new_model.create_record(values)
    nc_two = new_model.create_record({'an_integer': 1, 'a_long': 0L, 'a_bool': False, 'a_string': '', 'a_datetime': datetime.datetime.now()})
    nc_three = new_model.create_record({'an_integer': 10000, 'a_long': 9349234L, 'a_bool': False, 'a_string': '43132312', 'a_datetime': datetime.datetime.now()})
    nc_four = new_model.create_record({'a_datetime': datetime.datetime.now()})

    nc_delete_one = new_model.create_record({'a_datetime': datetime.datetime.now()})
    nc_delete_two = new_model.create_record({'a_datetime': datetime.datetime.now()})

    with pytest.raises(Exception):
        new_model.create_record()

    new_models_old = new_model.get_all()
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
    nc_four.set('an_integer', 10L)
    assert nc_four.get('an_integer') == 10

    nc_delete_three = new_model.create_record({'a_string': 'delete me'})
    assert len(new_model.get_all()) == len(new_models) + 1
    nc_delete_three.delete()
    assert len(new_model.get_all()) == len(new_models)

    # cannot add a record id column using an existing name
    with pytest.raises(ValueError):
        new_model.get_all().as_dataframe(
            record_id_column_name=new_model.get_all().type.schema.keys()[0]
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

    new_relationship = dataset.create_relationship_type('New_Relationship_{}'.format(current_ts()), 'a new relationship')

    assert len(dataset.relationships()) == len(relationships) + 1

    assert dataset.get_relationship(new_relationship.id) == new_relationship
    assert dataset.get_relationship(new_relationship.type) == new_relationship

    nr_one = new_relationship.relate(nc_one, nc_two)
    nr_four = new_relationship.relate(nc_four, nc_one)
    nr_five = new_relationship.relate(nc_four, nc_two)

    nr_two = nc_two.relate_to(nc_three, new_relationship)
    nr_three = nc_three.relate_to(nc_four, new_relationship)
    nr_six = nc_four.relate_to(nc_three, new_relationship)

    nc_four.update()
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

    nc_four.update()
    assert len(nc_four.get_related(new_model.type)) == 4

def test_simple_model_properties(dataset):

     # Define properties as tuples
     model_with_basic_props_1 = dataset.create_model('Basic_Props_1', description='a new description',schema=
     [('an_integer', int, 'An Integer', True), ('a_long', long), ('a_bool', bool), ('a_string', str), ('a_date', datetime.datetime)])

     assert dataset.get_model(model_with_basic_props_1.id) == model_with_basic_props_1

     # Add a property
     model_with_basic_props_1.add_property('a_new_property', float, display_name='Weight')
     model_with_basic_props_1.update()

     updated_model_1 = dataset.get_model(model_with_basic_props_1.id)

     assert updated_model_1.get_property('a_new_property').display_name == 'Weight'

     # Define properties as ModelProperty objects
     model_with_basic_props_2 = dataset.create_model('Basic_Props_2', description='a new description', schema=[
         ModelProperty('name', data_type=str, title=True, required=True),
         ModelProperty('age', data_type=int),
         ModelProperty('DOB', data_type=datetime.datetime)
     ])

     assert dataset.get_model(model_with_basic_props_2.id) == model_with_basic_props_2

     # Add a property
     model_with_basic_props_2.add_property('weight2', ModelPropertyType(data_type=float), display_name='Weight')
     model_with_basic_props_2.update()

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
     model_with_basic_props_3.update()

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
     model_with_basic_props_4.update()

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
    model_with_complex_props.update()

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
                      data_type=ModelPropertyEnumType(data_type=str, enum=['foo', 'bar', 'baz'], multi_select=True))
    ])

    result = dataset.get_model(model_with_enum_props.id)

    assert result == model_with_enum_props

    enum_property = result.get_property('some_enum')
    array_property = result.get_property('some_array')

    assert(enum_property.type == float)
    assert(enum_property.multi_select == False)
    assert(enum_property.enum == [1.0, 2.0, 3.0])
    assert (enum_property.unit == 'cm')

    assert (array_property.type == unicode)
    assert (array_property.multi_select == True)
    assert (array_property.enum == ['foo', 'bar', 'baz'])
