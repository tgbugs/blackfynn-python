'''
usage:
  bf [options] ( organizations | orgs )

global options:
  -h --help                 Show help
  --profile=<name>          Use specified profile (instead of default)
'''

from docopt import docopt
import sys

from cli_utils import get_client

def main():
    args = docopt(__doc__)

    bf = get_client()

    for o in bf.organizations():
        print "  {} (id: {})".format(o.name, o.id)
