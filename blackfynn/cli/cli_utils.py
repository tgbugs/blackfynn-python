
from blackfynn import settings
settings.using_cli = True

def get_item_path(bf, item):
    if item.parent is not None:
        parent = bf.get(item.parent)
        return get_item_path(bf, parent) + [item]
    else:
        dataset = bf.get_dataset(item.dataset)
        return [dataset, item]

def make_tree(path):
    if len(path)==1:
        value = path[0]
    else:
        value = make_tree(path[1:])
    return {path[0].id: value}

def merge(a, b, path=None):
    if path is None: path = []
    for key in b:
        if key in a:
            if isinstance(a[key], dict) and isinstance(b[key], dict):
                merge(a[key], b[key], path + [str(key)])
            elif a[key] == b[key]:
                pass # same leaf value
            else:
                raise Exception('Conflict at %s' % '.'.join(path + [str(key)]))
        else:
            a[key] = b[key]
    return a

def print_tree(tree, objects, indent=0):
    for key, value in tree.iteritems():
        is_dict = isinstance(value, dict)
        char = '-' if is_dict else '*'
        print ' '*indent+char, objects[key]
        if is_dict:
            print_tree(value, objects, indent=indent+2)

def print_path_tree(bf, results):
    paths = [get_item_path(bf, r) for r in results]
    path_objects = {o.id:o for p in paths for o in p}
    trees = [make_tree(path) for path in paths]
    print_tree(reduce(merge, trees), path_objects)

def print_collaborator_edit_resp(resp):
    for key, value in resp['changes'].iteritems():
        if value['success']:
            print " - {}: Success".format(key)
        else:
            print " - {}: Error - {}".format(key, value['message'])

def get_client():
    from blackfynn import Blackfynn

    try:
        bf = Blackfynn()
        return bf
    except:
        exit("Unable to authenticate against Blackfynn using the specified API token.")

def get_working_dataset(bf):
    ds = settings.working_dataset
    if ds:
        try:
            working_dataset = bf.get_dataset(ds)
            return working_dataset
        except Exception, e:
            exit(e)
    else:
        exit("Dataset required. You can set your working dataset with:" \
             "\n\n\tbf use <dataset>" \
             "\n\nor by specifying it in your command:" \
             "\n\n\t bf --dataset=<dataset> <command> ..." \
             "\n")

def print_datasets(bf):
    working_dataset = settings.working_dataset
    for dataset in bf.datasets():
        if dataset.id == working_dataset:
            print "\033[32m* {} (id: {})\033[0m".format(dataset.name, dataset.id)
        else:
            print "  {} (id: {})".format(dataset.name, dataset.id)

def get_item(identifier, bf):
    try:
        return bf.get(identifier)
    except:
        exit("{} does not exist.".format(identifier))
