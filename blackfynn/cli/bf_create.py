'''
usage:
  bf create [options] <name> [<destination>]

global options:
  -h --help                 Show help
  --dataset=<dataset>       Use specified dataset (instead of your current working dataset)
  --profile=<name>          Use specified profile (instead of default)
'''
from docopt import docopt
from blackfynn import Collection

from cli_utils import get_client, get_working_dataset, get_item

def main():
    args = docopt(__doc__)

    bf = get_client()

    collection = Collection(args['<name>'])

    if args['<destination>']:
        parent = get_item(args['<destination>'], bf)
        resp = parent.add(collection)
    else:
        dataset = get_working_dataset(bf)
        resp = dataset.add(collection)

    print(collection)
