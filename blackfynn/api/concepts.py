# -*- coding: utf-8 -*-
from blackfynn import settings
from blackfynn.api.base import APIBase
from blackfynn.models import (
    Concept, ConceptInstance, ConceptInstanceSet, ProxyInstance,
    Relationship, RelationshipInstance, RelationshipInstanceSet,
    DataPackage
)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Concepts
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class ConceptsAPIBase(APIBase):
    def _get_concept_type(self, concept, instance = None):
        if isinstance(concept, Concept):
            return concept.type
        elif isinstance(concept, basestring):
            return concept
        elif isinstance(instance, ConceptInstance):
            return instance.type
        else:
            raise Exception("could not get concept type from concept {} or instance {} ".format(concept, instance))

    def _get_relationship_type(self, relationship, instance = None):
        if isinstance(relationship, Relationship):
            return relationship.type
        elif isinstance(relationship, basestring):
            return relationship
        elif isinstance(instance, RelationshipInstance):
            return instance.type
        else:
            raise Exception("could not get relationship type from relationship {} or instance {} ".format(relationship, instance))

class ConceptsAPI(ConceptsAPIBase):
    host = settings.concepts_api_host
    base_uri = "/concepts"
    name = 'concepts'

    def __init__(self, session):
        self.instances = ConceptInstancesAPI(session)
        self.relationships = ConceptRelationshipsAPI(session)
        self.proxies = ConceptProxiesAPI(session)
        super(ConceptsAPI, self).__init__(session)

    def get(self, concept):
        concept_id = self._get_id(concept)
        r = self._get(self._uri('/{id}', id=concept_id))
        return Concept.from_dict(r, api=self.session)

    def update(self, concept):
        assert isinstance(concept, Concept), "concept must be type Concept"
        data = concept.as_dict()
        data['id'] = concept.id
        r = self._put(self._uri('/{id}', id=concept.id), json=data)
        return Concept.from_dict(r, api=self.session)

    def create(self, concept):
        assert isinstance(concept, Concept), "concept must be type Concept"
        r = self._post('', json=concept.as_dict())
        return Concept.from_dict(r, api=self.session)

    def get_all(self):
        resp = self._get('', stream=True)
        concepts = [Concept.from_dict(r, api=self.session) for r in resp]
        return { c.type: c for c in concepts }

    def delete_instances(self, concept, *instances):
        concept_id = self._get_id(concept)
        ids = [self._get_id(instance) for instance in instances]

        return self._del(self._uri('/{id}/instances', id=concept_id), json=ids)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Concept Instances
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class ConceptInstancesAPI(ConceptsAPIBase):
    host = settings.concepts_api_host
    base_uri = "/concepts"
    name = 'concepts.instances'

    def get(self, instance, concept=None):
        instance_id = self._get_id(instance)
        concept_type = self._get_concept_type(concept, instance)

        r = self._get(self._uri('/{c_type}/instances/{id}', c_type=concept_type, id=instance_id))
        return ConceptInstance.from_dict(r['node'], api=self.session)

    def neighbors(self, instance, concept=None):
        instance_id = self._get_id(instance)
        concept_type = self._get_concept_type(concept, instance)

        r = self._get(self._uri('/{c_type}/instances/{id}', c_type=concept_type, id=instance_id))
        concepts = r.get('concepts', list())
        proxies = r.get('proxyConcepts', list())

        neighbors = [ConceptInstance.from_dict(c, api=self.session) for c in concepts]
        neighbors += [ProxyInstance.from_dict(p, api=self.session) for p in proxies]

        return neighbors

    def relationships(self, instance, concept=None):
        instance_id = self._get_id(instance)
        concept_type = self._get_concept_type(concept, instance)

        r = self._get(self._uri('/{c_type}/instances/{id}', c_type=concept_type, id=instance_id))
        relationships = r.get('edges', list())

        return [RelationshipInstance.from_dict(r, api=self.session) for r in relationships]

    def get_all(self, concept):
        concept_type = self._get_concept_type(concept)

        resp = self._get(self._uri('/{c_type}/instances', c_type=concept_type), stream=True)
        instances = [ConceptInstance.from_dict(r, api=self.session) for r in resp]

        return ConceptInstanceSet(concept, instances)

    def delete(self, instance):
        assert isinstance(instance, ConceptInstance), "instance must be type ConceptInstance"
        r = self._del(self._uri('/{c_type}/instances/{id}', c_type=instance.type, id=instance.id))
        return r

    def create(self, instance):
        assert isinstance(instance, ConceptInstance), "instance must be type ConceptInstance"
        r = self._post(self._uri('/{c_type}/instances', c_type=instance.type), json=instance.as_dict())
        return ConceptInstance.from_dict(r, api=self.session)

    def update(self, instance):
        assert isinstance(instance, ConceptInstance), "instance must be type ConceptInstance"
        r = self._put(self._uri('/{c_type}/instances/{id}', c_type=instance.type, id=instance.id), json=instance.as_dict())
        return ConceptInstance.from_dict(r, api=self.session)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Relationships
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class ConceptRelationshipsAPI(ConceptsAPIBase):
    host = settings.concepts_api_host
    base_uri = "/relationships"
    name = 'concepts.relationships'

    def __init__(self, session):
        self.instances = ConceptRelationshipInstancesAPI(session)
        super(ConceptRelationshipsAPI, self).__init__(session)

    def create(self, relationship):
        assert isinstance(relationship, Relationship), "Must be of type Relationship"
        r = self._post('', json=relationship.as_dict())
        return Relationship.from_dict(r, api=self.session)

    def get(self, relationship):
        relationship_id = self._get_id(relationship)
        r = self._get(self._uri('/{r_id}', r_id=relationship_id))
        return Relationship.from_dict(r, api=self.session)

    def get_all(self):
        resp = self._get('', stream=True)
        relations = [Relationship.from_dict(r, api=self.session) for r in resp]
        return {r.type: r for r in relations}

class ConceptRelationshipInstancesAPI(ConceptsAPIBase):
    host = settings.concepts_api_host
    base_uri = "/relationships"
    name = 'concepts.relationships.instances'

    def get_all(self, relationship):
        relationship_id = self._get_id(relationship)

        resp = self._get( self._uri('/{r_id}/instances', r_id=relationship_id), stream=True)
        instances = [RelationshipInstance.from_dict(r, api=self.session) for r in resp]
        return RelationshipInstanceSet(relationship, instances)

    def get(self, instance, relationship=None):
        instance_id = self._get_id(instance)
        relationship_type = self._get_relationship_type(relationship, instance)
        r = self._get( self._uri('/{r_type}/instances/{id}', r_type=relationship_type, id=instance_id))
        return RelationshipInstance.from_dict(r, api=self.session)

    def delete(self, instance):
        assert isinstance(instance, RelationshipInstance), "instance must be of type RelationshipInstance"
        return self._del( self._uri('/{r_type}/instances/{id}', r_type=instance.type, id=instance.id))

    def link(self, relationship, source, destination, values=dict()):
        assert isinstance(source, (ConceptInstance, DataPackage)), "source must be an object of type ConceptInstance or DataPackage"
        assert isinstance(destination, (ConceptInstance, DataPackage)), "destination must be an object of type ConceptInstance or DataPackage"

        if isinstance(source, DataPackage):
            assert isinstance(destination, ConceptInstance), "DataPackages can only be linked to ConceptInstances"
            return self.session.concepts.proxies.create(source.id, relationship, destination, values, "ToConcept", "package")
        elif isinstance(destination, DataPackage):
            assert isinstance(source, ConceptInstance), "DataPackages can only be linked to ConceptInstances"
            return self.session.concepts.proxies.create(destination.id, relationship, source, values, "FromConcept", "package")
        else:
            relationship_type = self._get_relationship_type(relationship)
            values = [dict(name=k, value=v) for k,v in values.items()]
            instance = RelationshipInstance(type=relationship_type, source=source, destination=destination, values=values)
            return self.create(instance)

    def create(self, instance):
        assert isinstance(instance, RelationshipInstance), "instance must be of type RelationshipInstance"
        r = self._post( self._uri('/{r_type}/instances', r_type=instance.type), json=instance.as_dict())
        return RelationshipInstance.from_dict(r, api=self.session)

# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# Concept Proxies
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

class ConceptProxiesAPI(ConceptsAPIBase):
    host = settings.concepts_api_host
    base_uri = "/proxy"
    name = 'concepts.proxies'

    proxy_types = ["package"]
    direction_types = ["FromConcept", "ToConcept"]

    def get_all(self, proxy_type):
        r = self._get(self._uri('/{p_type}', p_type=proxy_type))
        return r

    def get(self, proxy_type, proxy_id):
        r = self._get(self._uri('/{p_type}/{p_id}', p_type=proxy_type, p_id=proxy_id))
        return r

    def create(self, external_id, relationship, concept_instance, values, direction = "ToConcept", proxy_type = "package", concept = None):
        assert proxy_type in self.proxy_types, "proxy_type must be one of {}".format(self.proxy_types)
        assert direction in self.direction_types, "direction must be one of {}".format(self.direction_types)

        concept_instance_id = self._get_id(concept_instance)
        concept_type = self._get_concept_type(concept, concept_instance)
        relationship_type = self._get_relationship_type(relationship)
        relationshipData = [dict(name=k, value=v) for k,v in values.items()]

        request = {}
        request['direction'] = direction
        request['externalId'] = external_id
        request['conceptType'] = concept_type
        request['conceptInstanceId'] = concept_instance_id
        request['relationshipType'] = relationship_type
        request['relationshipData'] = relationshipData

        r = self._post(self._uri('/{p_type}', p_type=proxy_type), json=request)
        return RelationshipInstance.from_dict(r['relationshipInstance'], api=self.session)
