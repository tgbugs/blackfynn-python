'''
usage:
  bf cache [options] clear
  bf cache [options] compact

global options:
  -h --help                 Show help
  --dataset=<dataset>       Use specified dataset (instead of your current working dataset)
  --profile=<name>          Use specified profile (instead of default)
'''

from docopt import docopt

def main():
    args = docopt(__doc__)

    from blackfynn.cache import get_cache
    cache = get_cache(init=False)
	
    if args['clear']:
        print "Clearing cache..."
        cache.clear()
        print "Cache cleared."
    elif args['compact']:
        print 'Compacting cache...'
        cache.start_compaction(async=False)
        print 'Cache compaction done.'
