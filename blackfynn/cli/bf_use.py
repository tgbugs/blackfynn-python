'''
usage:
  bf use [options] <dataset>

global options:
  -h --help                 Show help
  --profile=<name>          Use specified profile (instead of default)
'''

from docopt import docopt

from cli_utils import get_client, settings

def main():
    args = docopt(__doc__)

    bf = get_client()
    dataset_id_or_name = args['<dataset>']

    try:
        dataset = bf.get_dataset(dataset_id_or_name)
        settings.set_working_dataset(dataset)
    except Exception, e:
        exit(e)
