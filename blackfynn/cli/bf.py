'''usage:
  bf [options] [<command>] [<args>...]

Available commands:
  use               Set your current working dataset
  init              Initialize a new dataset
  search            Search across your datasets
  datasets          List your datasets
  organizations     List the organizations you belong to

  collaborators     List the collaborators of the current working dataset
  share             Share a dataset with users, teams, or your organization
  unshare           Revoke access to the a dataset from users, teams, or your organization

  props             Add/remove/modify a package or collection's properties
  move              Move a package or collection
  where             Show path to package or collection
  append            Append data to a package
  rename            Rename a package or collection
  delete            Delete a package or collection
  create            Create a collection
  get               Get the contents of a package, collection, or dataset

  upload            Upload file(s) or directory

  status            Display connection status
  cache             Perform cache operations
  profile           Profile management


global options:
  -h --help                 Show help
  --dataset=<dataset>       Use specified dataset (instead of your current working dataset)
  --profile=<name>          Use specified profile (instead of default)
'''

from docopt import docopt
import os

import blackfynn
from blackfynn import Blackfynn
from cli_utils import settings, load_evars

def blackfynn_cli():
    args = docopt(__doc__,
                  version='bf version {}'.format(blackfynn.__version__),
                  options_first=True)

    # Test for these two commands first as they
    # do not require a Blackfynn client
    if args['<command>'] in ['help',None]:
        print(__doc__.strip('\n'))
        return

    if args['<command>'] == 'profile':
        import bf_profile
        bf_profile.main()
        return

    # Display warning message if config.ini is not found
    if not os.path.exists(settings.config_file):
        print("\033[31m* Warning: No config file found, run 'bf profile' to start the setup assistant\033[0m")

    # Try to use profile specified by --profile, exit if invalid
    overrides, _ = load_evars()
    try:
        bf = Blackfynn(args['--profile'], overrides)
    except Exception, e:
        exit(e)

    #Try to use dataset specified by --dataset, exit if invalid
    try:
        if args['--dataset'] is not None:
            from cli_utils import get_client
            bf = get_client()
            dataset = bf.get_dataset(args['--dataset'])
            settings.set_working_dataset(dataset.id)
    except Exception, e:
        exit(e)

    if args['<command>'] == 'status':
        import bf_status
        bf_status.main(bf)
    elif args['<command>'] == 'use':
        import bf_use
        bf_use.main(bf)
    elif args['<command>'] == 'init':
        import bf_init
        bf_init.main(bf)
    elif args['<command>'] in ['datasets', 'ds']:
        import bf_datasets
        bf_datasets.main(bf)
    elif args['<command>'] in ['organizations', 'orgs']:
        import bf_organizations
        bf_organizations.main(bf)
    elif args['<command>'] in ['share', 'unshare', 'collaborators']:
        import bf_share
        bf_share.main(bf)
    elif args['<command>'] == 'cache':
        import bf_cache
        bf_cache.main(bf)
    elif args['<command>'] == 'create':
        import bf_create
        bf_create.main(bf)
    elif args['<command>'] == 'delete':
        import bf_delete
        bf_delete.main(bf)
    elif args['<command>'] == 'move':
        import bf_move
        bf_move.main(bf)
    elif args['<command>'] == 'rename':
        import bf_rename
        bf_rename.main(bf)
    elif args['<command>'] == 'props':
        import bf_props
        bf_props.main(bf)
    elif args['<command>'] == 'get':
        import bf_get
        bf_get.main(bf)
    elif args['<command>'] == 'where':
        import bf_where
        bf_where.main(bf)
    elif args['<command>'] == 'upload':
        import bf_upload
        bf_upload.main(bf)
    elif args['<command>'] == 'append':
        import bf_append
        bf_append.main(bf)
    elif args['<command>'] == 'search':
        import bf_search
        bf_search.main(bf)
    else:
        exit("Invalid command: '{}'\nSee 'bf help' for available commands".format(args['<command>']))
