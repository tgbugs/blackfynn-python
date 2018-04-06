'''
usage:
  bf [options] ( organizations | orgs )

global options:
  -h --help                 Show help
  --profile=<name>          Use specified profile (instead of default)
'''

from docopt import docopt
import sys

def main(bf):
    args = docopt(__doc__)

    for o in bf.organizations():
        print "  {} (id: {})".format(o.name, o.id)
