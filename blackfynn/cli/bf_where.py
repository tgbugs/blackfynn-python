'''
usage:
  bf where [options] <item>

global options:
  -h --help                 Show help
  --dataset=<dataset>       Use specified dataset (instead of your current working dataset)
  --profile=<name>          Use specified profile (instead of default)
'''

from docopt import docopt
from cli_utils import print_path_tree, get_item

def main(bf):
    args = docopt(__doc__)

    item = get_item(args['<item>'], bf)
    print_path_tree(bf, [item])
