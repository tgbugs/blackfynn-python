def test_endpoints(simple_graph):
    dataset = simple_graph.dataset

    dataset.create_graph_view('My view', 'patient', ['medication'])
