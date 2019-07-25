'''
description:
  The profile management system enables the user to easily create multiple profiles for easily
  switching between accounts or organizations. The user can also set/unset global settings which
  apply to all profiles.

  Settings are loaded in the following order, with each tier overriding the last:
    1. Global settings - global settings are loaded first
    2. Default profile - if set, the default profile is loaded
    3. Command line arguments - e.g.: --profile=<name>
    4. Environment variables - e.g.: BLACKFYNN_API_TOKEN

usage:
  bf_profile [options] [<command>] [<args>...]

commands:
  create <name>              Create new profile
  delete <name>              Delete profile
  set-default (none|<name>)  Set default profile (or none).
  [-c --contents] list       List profiles (and optionally their contents), also highlights
                             default profile.
  show (global|<name>)       Show ALL settings for a profile
  status                     Show connection status

global options:
  -c --contents              List profiles with contents. Also lists global settings, if any
  -f --force                 Attempt action without prompting for confirmation. Use with care
  -h --help                  Show help
  --profile=<name>           Use specified profile (instead of default)

advanced commands:
  set (global|<name>) <key> <value>   Set key/value pair for given profile, or globally
  unset (global|<name>) <key>         Unset key/value pair for given profile, or globally
  keys                                List all available keys and their default values

For additional features, install the Blackfynn Agent:
https://developer.blackfynn.io/agent
'''
from __future__ import absolute_import, print_function
import io
import os
from builtins import input

from docopt import docopt

import blackfynn
from blackfynn import Blackfynn, DEFAULT_SETTINGS, Settings


def main():
    args = docopt(__doc__,
                  version='bf_profile version {}'.format(blackfynn.__version__),
                  options_first=True)

    # Test for these two commands first as they
    # do not require a Blackfynn client
    if args['<command>'] in ('help', None):
        print(__doc__.strip('\n'))
        return

    settings = Settings(args['--profile'])
    if not os.path.exists(settings.config_file):
        setup_assistant(settings)

    elif args['<command>'] == 'create'      : create_profile(settings, args['<args>'])
    elif args['<command>'] == 'delete'      : delete_profile(settings, args['<args>'], args['--force'])
    elif args['<command>'] == 'set-default' : set_default(settings, args['<args>'])
    elif args['<command>'] == 'list'        : list_profiles(settings, args['--contents'])
    elif args['<command>'] == 'show'        : show_profile(settings, args['<args>'])

    elif args['<command>'] == 'set'         : set_key(settings, args['<args>'], args['--force'])
    elif args['<command>'] == 'unset'       : unset_key(settings, args['<args>'], args['--force'])
    elif args['<command>'] == 'keys'        : list_keys(settings)

    elif args['<command>'] == 'status':
        bf = Blackfynn(args['--profile'])
        show_status(bf)

    else:
        invalid_usage()

    with io.open(settings.config_file, 'w') as configfile:
        settings.config.write(configfile)

def setup_assistant(settings):
    settings.config.clear()

    print("Blackfynn profile setup assistant")

    settings.config['global'] = {'default_profile' : 'none'}

    print("Create a profile:")
    create_profile(settings)

    print("Setup complete. Enter 'bf profile help' for available commands and actions")


#User commands
#=======================================================
def create_profile(settings, args=None):
    if not args:
        name = input('  Profile name [default]: ') or 'default'

    elif len(args) == 1:
        name = args[0]

    else:
        invalid_usage()

    if name in settings.config:
        if name in ('global', 'agent', 'none'):
            print("Profile name '{}' reserved for system. Please try a different name".format(name))
        else:
            print("Profile '{}' already exists".format(name))
    else:
        print("Creating profile '{}'".format(name))
        settings.config[name] = {}

        settings.config[name]['api_token']  = input('  API token: ')
        settings.config[name]['api_secret'] = input('  API secret: ')

        if settings.config['global']['default_profile'] == 'none':
            settings.config['global']['default_profile'] = name
            print("Default profile: {}".format(name))

        else:
            if yesno_prompt("Would you like to set '{}' as default (Y/n)? ".format(name)):
                set_default(settings, [name])

def delete_profile(settings, name, force):
    if len(name) != 1:
        invalid_usage()
    else: name = name[0]

    if valid_name(settings, name):
        if not force:
            force = yesno_prompt("Delete profile '{}' (Y/n)? ".format(name))

        if force:
            print("Deleting profile '{}'".format(name))
            settings.config.remove_section(name)

            if settings.config['global']['default_profile'] == name:
                settings.config['global']['default_profile'] = 'none'
                print("\033[31m* Warning: default profile unset. Use 'bf profile set-default <name>' to set a new default\033[0m")
        else:
            print('abort')

def list_profiles(settings, contents):
    '''
    Lists all profiles
    '''
    print('Profiles:')
    for section in settings.config.sections():
        if section not in ['global']:
            if section == settings.config['global']['default_profile']:
                print('* \033[32m{}\033[0m'.format(section))
            else:
                print('  {}'.format(section))
            if contents:
                print_profile(settings, section, 4)
    if contents:
        if len(settings.config['global']) > 1:
            print('Global Settings:')
            print_profile(settings, 'global', 2)


def set_default(settings, name):
    if len(name) != 1:
        invalid_usage()
    else: name = name[0]

    if name == 'none':
        print("Default profile unset. Using global settings and environment variables")
        settings.config['global']['default_profile'] = 'none'
    elif valid_name(settings, name):
        print("Default profile: {}".format(name))
        settings.config['global']['default_profile'] = name

def show_profile(settings, name):
    if len(name) != 1:
        invalid_usage()
    else: name = name[0]

    if name == 'global' or valid_name(settings, name):
        print('{} contents:'.format(name))
        print_profile(settings, name, 2, True)


#Advanced commands
#=======================================================
def set_key(settings, args, force):
    if len(args) != 3:
        invalid_usage()

    name, key, value = args

    if not key in DEFAULT_SETTINGS:
        print("Invalid key: '{}'\n see 'bf profile keys' for available keys".format(key))
        return

    if not name in settings.config:
        print("Profile '{}' does not exist".format(name))
        return

    if not force and key in settings.config[name]:
        force = yesno_prompt("{}: {} already set. Overwrite (Y/n)? ".format(name, key))
    else: force = True

    if force:
        print("{}: {}={}".format(name, key, value))
        settings.config[name][key] = value

def unset_key(settings, args, force):
    if len(args) != 2:
        invalid_usage()

    name, key = args

    if not name in settings.config:
        print("Profile '{}' does not exist".format(name))
        return

    if not key in settings.config[name]:
        print("{}: {} not set".format(name, key))
        return

    if not force:
        if key in settings.config[name]:
            force = yesno_prompt("{}: Unset {} (Y/n)? ".format(name, key))

    if force:
        print("{}: {} unset".format(name, key))
        settings.config[name].pop(key)

def list_keys(settings):
    default = settings.default_profile
    print('Keys and default values:')
    for key, value in sorted(settings.profiles[default].items()):
        print('  {} : {}'.format(key, value))

def show_status(bf):
    print('Active profile:\n  \033[32m{}\033[0m\n'.format(bf.settings.active_profile))

    if bf.settings.env:
        key_len = 0
        value_len = 0
        for key, value in bf.settings.env.items():
            valstr = '{}'.format(value)
            key_len = max(key_len, len(key))
            value_len = max(value_len, len(valstr))

        print('Environment variables:')
        print('  \033[4m{:{key_len}}    {:{value_len}}    {}'.format(
            'Key', 'Value', 'Environment Variable\033[0m', key_len=key_len, value_len=value_len))
        for value, evar in sorted(bf.settings.env.items()):
            valstr = '{}'.format(value)
            # get internal variable
            ivar = ''
            for k, v in bf.settings.__dict__.items():
                if str(v) == str(evar):
                    ivar = k
            print('  {:{key_len}}    {:{value_len}}    {}'.format(
                ivar, evar, valstr, key_len=key_len, value_len=value_len))
        print()

    print("Blackfynn environment:")
    print("  User               : {}".format(bf.profile.email))
    print("  Organization       : {} (id: {})".format(bf.context.name, bf.context.id))
    print("  API Location       : {}".format(bf.settings.api_host))
    print("  Streaming API      : {}".format(bf.settings.streaming_api_host))


#Helper functions
#=======================================================
def yesno_prompt(msg):
    return input(msg).lower() in ('y', 'yes')

def invalid_usage():
    print('Invalid usage. See bf profile help for available commands')
    exit()

def valid_name(settings, name):
    if name not in settings.config or name == 'global':
        print("Profile '{}' does not exist".format(name))
        return False
    return True

def print_profile(settings, name, indent=0, show_all=False):
    if show_all:
        key_len = 0
        for key in settings.profiles[name].keys():
            key_len = max(key_len, len(key))

        for key, value in sorted(settings.profiles[name].items()):
            if key != 'default_profile':
                if key in settings.config[name] and name != 'global':
                    print(' '*indent + '{:{key_len}} : \033[32m{}\033[0m ({})'.format(key, value, name, key_len=key_len))
                elif key in settings.config['global']:
                    print(' '*indent + '{:{key_len}} : \033[34m{}\033[0m (global)'.format(key, value, key_len=key_len))
                else:
                    print(' '*indent + '{:{key_len}} : \033[0m{}\033[0m'.format(key, value, key_len=key_len))

    else:
        key_len = 0
        for key in settings.config[name].keys():
            key_len = max(key_len, len(key))

        for key, value in sorted(settings.config[name].items()):
            if key != 'default_profile':
                print(' '*indent + '{:{key_len}} : {}'.format(key, value, key_len=key_len))
