'''
usage:
  bf search [options] <term>...

search options:
  --show-paths

global options:
  -h --help                 Show help
  --dataset=<dataset>       Use specified dataset (instead of your current working dataset)
  --profile=<name>          Use specified profile (instead of default)
'''

from docopt import docopt
from cli_utils import print_path_tree, get_client

def main():
    args = docopt(__doc__)

    bf = get_client()

    terms = ' '.join(args['<term>'])
    results = bf._api.search.query(terms)
    if len(results) == 0:
        print "No Results."
    else:
        if args['--show-paths']:
            print_path_tree(bf, results)
        else:
            for r in results:
                print " * {}".format(r)
