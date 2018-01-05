'''
usage:
  bf status [options]

global options:
  -h --help                 Show help
  --dataset=<dataset>       Use specified dataset (instead of your current working dataset)
  --profile=<name>          Use specified profile (instead of default)
'''
from docopt import docopt

from cli_utils import get_client, settings

def main():
    args = docopt(__doc__)

    print('Active profile:\n  \033[32m{}\033[0m\n'.format(settings.active_profile))

    if len(settings.eVars.keys()) > 0:
        key_len = 0
        value_len = 0
        for key, (value,_) in settings.eVars.items():
            valstr = '{}'.format(value)
            key_len = max(key_len,len(key))
            value_len = max(value_len,len(valstr))
        
        print('Environment variables:')
        print('  \033[4m{:{key_len}}    {:{value_len}}    {}'.format('Key','Value','Environment Variable\033[0m',key_len=key_len,value_len=value_len))
        for key, (value,evar) in sorted(settings.eVars.items()):
           valstr = '{}'.format(value)
           print('  {:{key_len}}    {:{value_len}}    {}'.format(key,valstr,evar,key_len=key_len,value_len=value_len))
        print
           
    bf = get_client()

    ds = settings.working_dataset
    if ds:
        try:
            working_dataset = bf.get_dataset(ds)
            working_dataset_status = "{} (id: {})".format(working_dataset.name, working_dataset.id)
        except:
            working_dataset_status = 'Not set.'
    else:
        working_dataset_status = 'Not set.'

    print "Blackfynn environment:"
    print "  User               : {}".format(bf.profile.email)
    print "  Organization       : {} (id: {})".format(bf.context.name, bf.context.id)
    print "  Dataset            : {}".format(working_dataset_status)
    print "  API Location       : {}".format(bf.host)
    print "  Streaming API      : {}".format(settings.streaming_api_host)
