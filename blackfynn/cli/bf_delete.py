'''
usage:
  bf delete [options] <item>

global options:
  -h --help                 Show help
  --dataset=<dataset>       Use specified dataset (instead of your current working dataset)
  --profile=<name>          Use specified profile (instead of default)
'''

from docopt import docopt
from blackfynn import DataPackage, Collection

from cli_utils import get_item

def main(bf):
    args = docopt(__doc__)

    try:
        item = get_item(args['<item>'], bf)

        if not (isinstance(item, Collection) or isinstance(item, DataPackage)):
            raise Exception("only data packages and collections may be deleted.")

        bf.delete(item)
    except Exception, e:
        exit("  failed to delete {}: {}".format(args['<item>'], e))
