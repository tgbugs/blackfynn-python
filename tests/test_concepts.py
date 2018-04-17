import time
import pytest
import datetime

from blackfynn.models import Concept, ConceptInstance, DataPackage, Relationship, RelationshipInstance

def test_concepts(dataset):
    current_ts = lambda: int(round(time.time() * 1000))
    schema = {'an_integer': int, 'a_long': long, 'a_bool': bool, 'a_string': str, 'a_datetime': datetime.datetime}
    metadata = {'displayName': True}
    description = 'a new description'
    values = {'an_integer': 100, 'a_long': 100000L, 'a_bool': True, 'a_string': 'fnsdlkn#$#42nlfds$3nlds$#@$23fdsnfkls', 'a_datetime': datetime.datetime.now()}

    #################################
    ## Concepts
    ################################

    concepts = dataset.concepts()

    new_concept = dataset.create_concept('New_Concept_{}'.format(current_ts()), 'a new concept', schema)

    assert len(dataset.concepts()) == len(concepts) + 1

    assert dataset.get_concept(new_concept.id) == new_concept
    assert dataset.get_concept(new_concept.type) == new_concept

    new_concept.add_property('a_new_property', str, metadata)
    new_concept.description = description
    new_concept.update()

    new_concept = dataset.get_concept(new_concept.id)

    assert new_concept.description == description
    assert new_concept.get_property('a_new_property').metadata == metadata

    new_concept.add_properties([('a_new_float', float), {'name': 'a_new_int', 'data_type': int}, 'a_new_string'])
    new_concept.get_property('a_new_float').type == float
    new_concept.get_property('a_new_int').type == int
    new_concept.get_property('a_new_string').type == basestring

    nc_one = new_concept.create(values)
    nc_two = new_concept.create({'an_integer': 1, 'a_long': 0L, 'a_bool': False, 'a_string': '', 'a_datetime': datetime.datetime.now()})
    nc_three = new_concept.create({'an_integer': 10000, 'a_long': 9349234L, 'a_bool': False, 'a_string': '43132312', 'a_datetime': datetime.datetime.now()})
    nc_four = new_concept.create({'a_datetime': datetime.datetime.now()})

    nc_delete_one = new_concept.create({'a_datetime': datetime.datetime.now()})
    nc_delete_two = new_concept.create({'a_datetime': datetime.datetime.now()})

    try:
        new_concept.create()
        assert False
    except:
        assert True

    new_concepts_old = new_concept.get_all()
    new_concept.delete_items(nc_delete_one, nc_delete_two.id)
    new_concepts = new_concept.get_all()

    assert len(new_concepts) == (len(new_concepts_old) - 2)

    assert nc_two.concept() == new_concept
    assert nc_two.get('a_string') == ''

    nc_four.set('a_string', 'hello')
    assert nc_four.get('a_string') == new_concept.get(nc_four).get('a_string')

    try:
        nc_four.set('an_integer', datetime.datetime.now())
        assert False
    except:
        assert True

    assert nc_four.get('an_integer') == None
    nc_four.set('an_integer', 10L)
    assert nc_four.get('an_integer') == 10

    nc_delete_three = new_concept.create({'a_string': 'delete me'})
    assert len(new_concept.get_all()) == len(new_concepts) + 1
    nc_delete_three.delete()
    assert len(new_concept.get_all()) == len(new_concepts)

    df_cs = new_concept.get_all().as_dataframe()

    #################################
    ## Relationships
    ################################

    relationships = dataset.relationships()

    new_relationship = dataset.create_relationship('New_Relationship_{}'.format(current_ts()), 'a new relationship', schema)

    assert len(dataset.relationships()) == len(relationships) + 1

    assert dataset.get_relationship(new_relationship.id) == new_relationship
    assert dataset.get_relationship(new_relationship.type) == new_relationship

    assert new_relationship.get_property('a_datetime').type == datetime.datetime

    try:
        new_relationship.update()
        assert False
    except:
        assert True

    try:
        new_relationship.add_property('a_new_property', str, metadata)
        assert False
    except:
        assert True

    try:
        new_relationship.add_properties([('a_new_float', float), {'name': 'a_new_int', 'data_type': int}, 'a_new_string'])
        assert False
    except:
        assert True

    nr_one = new_relationship.link(nc_one, nc_two, values)
    nr_two = nc_two.link(new_relationship, nc_three, {'an_integer': 1, 'a_long': 0L, 'a_bool': False, 'a_string': '', 'a_datetime': datetime.datetime.now()})
    nr_three = nc_three.link(new_relationship.type, nc_four, {'an_integer': 10000, 'a_long': 9349234L, 'a_bool': False, 'a_string': '43132312', 'a_datetime': datetime.datetime.now()})
    nr_four = new_relationship.link(nc_four, nc_one, {'a_datetime': datetime.datetime.now()})
    nr_five = new_relationship.link(nc_four, nc_two)
    nr_six = nc_four.link(new_relationship, nc_three)

    nc_four.update()
    assert len(nc_four.relationships()) == 4
    assert len(nc_four.neighbors()) == 4
    assert len(nc_four.links()) == 4
    assert len(nc_four.relationships(new_relationship.type)) == 4
    assert len(nc_four.neighbors(new_relationship.type)) == 4
    assert len(nc_four.links(new_relationship.type)) == 4

    try:
        new_relationship.link(nc_one, nc_two, {'bad_property': False})
        assert False
    except:
        assert True
    
    new_relationships = new_relationship.get_all()

    assert nr_two.relationship() == new_relationship
    assert nr_two.get('a_string') == ''

    try:
        nr_four.set('an_integer', datetime.datetime.now())
        assert False
    except:
        assert True

    assert nr_four.get('an_integer') == None

    nr_delete_three = new_relationship.link(nc_one, nc_two, {'a_string': 'delete me'})
    assert len(new_relationship.get_all()) == len(new_relationships) + 1
    nr_delete_three.delete()
    assert len(new_relationship.get_all()) == len(new_relationships)

    try:
        new_relationship.link(nc_one, nc_one)
        assert False
    except:
        assert True

    df_rs = new_relationship.get_all().as_dataframe()

    p = DataPackage('test-csv', package_type='Tabular')
    dataset.add(p)
    dataset.update()
    assert p.exists

    p.link(new_relationship, nc_one)
    p.link(new_relationship.type, nc_two)
    nc_three.link(new_relationship, p)
    new_relationship.link(nc_four, p)

    nc_four.update()
    assert len(nc_four.relationships()) == 5
    assert len(nc_four.neighbors()) == 5
    assert len(nc_four.links()) == 5
    assert len(nc_four.relationships(new_relationship.type)) == 5
    assert len(nc_four.neighbors(new_relationship.type)) == 5
    assert len(nc_four.links(new_relationship.type)) == 5
