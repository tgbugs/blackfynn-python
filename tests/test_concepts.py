import time
import pytest
import datetime

from blackfynn.models import Model, Record, DataPackage, RelationshipType, Relationship

def current_ts():
    return int(round(time.time() * 1000))


def test_model_with_invalid_properties(dataset):
    invalid_schema = [('an_integer', int, 'An Integer')]
    new_model = dataset.create_model(
        'New_Model_{}'.format(current_ts()), 'A New Model', 'a new model', invalid_schema
    )

    assert not new_model.schema


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
