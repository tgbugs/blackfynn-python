'''
usage:
  bf [options] init <name>

global options:
  -h --help                 Show help
  --profile=<name>          Use specified profile (instead of default)
'''

from docopt import docopt
import sys

from cli_utils import print_datasets
from working_dataset import set_working_dataset

def main(bf):
    args = docopt(__doc__)

    name = args['<name>']
    dataset = bf.create_dataset(name)

    set_working_dataset(dataset)

    print_datasets(bf)
