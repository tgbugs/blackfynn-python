'''
usage:
  bf [options] init <name>

global options:
  -h --help                 Show help
  --profile=<name>          Use specified profile (instead of default)
'''

from docopt import docopt
import sys

from cli_utils import get_client, print_datasets, settings

def main():
    args = docopt(__doc__)

    bf = get_client()

    name = args['<name>']
    dataset = bf.create_dataset(name)

    settings.set_working_dataset(dataset)

    print_datasets(bf)
