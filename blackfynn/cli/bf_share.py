'''
usage:
  bf collaborators [options]
  bf share [options] <collaborators>...
  bf unshare [options] <collaborators>...

global options:
  -h --help                 Show help
  --dataset=<dataset>       Use specified dataset (instead of your current working dataset)
  --profile=<name>          Use specified profile (instead of default)
'''
from __future__ import absolute_import, division, print_function

import sys


from docopt import docopt

from .working_dataset import require_working_dataset


def main(bf):
    args = docopt(__doc__)

    ds = require_working_dataset(bf)

    if args['collaborators']:
        resp = ds.collaborators

        if resp['users'] or resp['organizations']:
            print(" - Users")
            for u in resp['users']:
                print("   - email:{} id:{}".format(u.email, u.id))

            print(" - Organizations")
            for o in resp['organizations']:
                print("   - name:{} id:{}".format(o.name, o.id))
        else:
            print("  No collaborators.")

    elif args['share']:
        ids = args['<collaborators>']

        resp = ds.add_collaborators(*ids)
        print_collaborator_edit_resp(resp)

    elif args['unshare']:
        ids = args['<collaborators>']

        resp = ds.remove_collaborators(*ids)
        print_collaborator_edit_resp(resp)

def print_collaborator_edit_resp(resp):
    for key, value in list(resp['changes'].items()):
        if value['success']:
            print(" - {}: Success".format(key))
        else:
            print(" - {}: Error - {}".format(key, value['message']))
