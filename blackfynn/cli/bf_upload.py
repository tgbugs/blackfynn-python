'''
usage:
  bf upload [options] [--to=destination] <file>...

global options:
  -h --help                 Show help
  --dataset=<dataset>       Use specified dataset (instead of your current working dataset)
  --profile=<name>          Use specified profile (instead of default)
'''

from docopt import docopt
from cli_utils import recursively_upload, get_client, settings
import os

def main():
    args = docopt(__doc__)

    bf = get_client()

    files = args['<file>']

    if args['--to']:
        collection = bf.get(args['--to'])
        recursively_upload(bf, collection, files)

    else:
        ds = settings.working_dataset
        if not ds:
          exit("ERROR: Must specify destination when uploading. Options:\n" \
               "\n   1. Set destination explicitly using --to command line argument" \
               "\n   2. Set default dataset using 'bf use <dataset>' before running upload command" \
               "\n")
        dataset = bf.get_dataset(ds)
        recursively_upload(bf, dataset, files)
