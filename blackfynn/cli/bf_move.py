'''
usage:
  bf move [options] <item> [<destination>]

global options:
  -h --help                 Show help
  --dataset=<dataset>       Use specified dataset (instead of your current working dataset)
  --profile=<name>          Use specified profile (instead of default)
'''

from docopt import docopt
from blackfynn import Dataset
from blackfynn.models import Collection
import blackfynn.log as log

from cli_utils import get_item

logger = log.get_logger('blackfynn.cli.bf_move')

def main(bf):
    args = docopt(__doc__)

    item = get_item(args['<item>'], bf)

    if args['<destination>']:
        destination = get_item(args['<destination>'], bf)
    else:
        # item will be moved to to the top of its containing dataset
        destination = None

    if destination is None or isinstance(destination, Collection):
        try:
            bf.move(destination, item)
        except Exception, e:
            logger.error(e)
            exit("Failed to move {} into {}.".format(item, destination))
    else:
        exit("Destination must be a collection.")
