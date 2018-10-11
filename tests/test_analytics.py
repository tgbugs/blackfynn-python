import uuid


def test_create_graph_view(simple_graph):
    dataset = simple_graph.dataset
    name = 'patient-view-{}'.format(uuid.uuid4())

    view = dataset.create_graph_view(name, 'patient', ['medication'])
    assert view.name == name
    assert view.root_model == 'patient'
    assert view.included_models == ['medication']

    # # TODO: update this to retrieve by name
    # view = dataset.get_graph_view(view.id)
    # assert view.name == name
    # assert view.root_model == 'patient'
    # assert view.included_models == ['medication']
