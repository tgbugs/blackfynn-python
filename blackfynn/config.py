import os
import sys
import psutil
import tempfile
import configparser

class Settings(object):
    def __init__(self, profile=None, overrides=None):
        self.profiles = {}

        self._load_defaults()
        self._load_global()
        self._load_profiles()

        # Load default profile first
        try:
            self._switch_profile(self.config['global']['default_profile'])
        except:
            self._switch_profile('global')

        # Load specific profile if it is specified
        self._switch_profile(profile)

        self._update(overrides)

        if not os.path.exists(self.cache_dir) and self.use_cache:
            os.makedirs(self.cache_dir)

        self.using_cli = False

    def _load_defaults(self):
        # GET/MAKE REQUIRED DIRECTORIES/FILES
        #=============================================
        self.blackfynn_dir = os.environ.get('BLACKFYNN_LOCAL_DIR', os.path.join(os.path.expanduser('~'), '.blackfynn'))
        if not os.path.exists(self.blackfynn_dir):
            os.makedirs(self.blackfynn_dir)

        self.cache_dir = os.environ.get('BLACKFYNN_CACHE_LOC', os.path.join(self.blackfynn_dir,'cache'))

        # GET CONFIG OBJECT
        #============================================
        self.config_file = os.path.join(self.blackfynn_dir,'config.ini')
        self.config = configparser.ConfigParser()
        self.config.read(self.config_file)


        # DEFAULT SETTINGS
        #=============================================
        self.defaults = {
            # blackfynn api locations
            'api_host'                    : 'https://api.blackfynn.io',
            'streaming_api_host'          : 'https://streaming.blackfynn.io',

            # blackfynn API token/secret
            'api_token'                   : None,
            'api_secret'                  : None,

            # streaming
            'stream_name'                 : 'prod-stream-blackfynn',
            'stream_aws_region'           : 'us-east-1',
            'stream_max_segment_size'     : 5000,

            # all requests
            'max_request_time'            : 120, # two minutes
            'max_request_timeout_retries' : 2,

            #io
            'max_upload_workers'          : 10,

            # timeseries
            'max_points_per_chunk'        : 10000,

            # s3 (amazon/local)
            's3_host'                     : '',
            's3_port'                     : '',

            # logging
            'log_level'                   : 'INFO',

            # cache
            'cache_index'                 : os.path.join(self.cache_dir, 'index.db'),
            'cache_max_size'              : 2048,
            'cache_inspect_interval'      : 1000,
            'ts_page_size'                : 3600,
            'use_cache'                   : True,
        }

    # _update safely updates settings to the internal __dict__
    def _update(self, settings):
        if settings is None or type(settings) != dict:
            return
        for k in self.defaults:
            if k in settings:
                self.__dict__[k] = settings[k]

    @property
    def host(self):
        return self.api_host

    @property
    def streaming_host(self):
        return self.streaming_api_host

    @property
    def _working_dataset_file(self):
        try:
            if os.name == 'nt':
                parent_shell_pid = psutil.Process(os.getpid()).parent().parent().pid
            else:
                parent_shell_pid = psutil.Process(os.getpid()).parent().pid
        except:
            return None
        fname = 'blackfynn_working_dataset_{}'.format(parent_shell_pid)
        return os.path.join(tempfile.gettempdir(), fname)

    @property
    def working_dataset(self):
        """
        Note: only works when using CLI
        """
        if not self.using_cli:
            return None
        ds_file = self._working_dataset_file
        if os.path.exists(ds_file):
            try:
                with open(ds_file, 'r+') as f:
                    ds_id = f.read().strip()
                    return ds_id
            except:
                pass
        return None

    def set_working_dataset(self, dataset):
        if not self.using_cli:
            return None
        ds_file = self._working_dataset_file
        try:
            with open(ds_file, 'w+') as f:
                f.write(dataset.id)
            # double check
            assert self.working_dataset is not None, "Error writing to working dataset file"
        except:
            print "We encountered an error while setting your working dataset.\n\n" + \
                  "Please use the --dataset flag where appropriate, instead."
        return None

    def _load_global(self):
        self.default_profile = None
        self.profiles['global'] = self.defaults.copy()
        if 'global' in self.config:
            for key, value in self.config['global'].items():
                if value == 'none'            : self.profiles['global'][key] = None
                elif value.lower() == 'true'  : self.profiles['global'][key] = True
                elif value.lower() == 'false' : self.profiles['global'][key] = False
                elif value.isdigit()          : self.profiles['global'][key] = int(value)
                else                          : self.profiles['global'][key] = str(value)

    def _load_profiles(self):
        for name in self.config.sections():
            if name is not 'global':
                self.profiles[name] = self.profiles['global'].copy()
                for key, value in self.config[name].items():
                    if value == 'none'            : self.profiles[name][key] = None
                    elif value.lower() == 'true'  : self.profiles[name][key] = True
                    elif value.lower() == 'false' : self.profiles[name][key] = False
                    elif value.isdigit()          : self.profiles[name][key] = int(value)
                    else                          : self.profiles[name][key] = str(value)

    def _switch_profile(self, name):
        if name is None:
            return
        if name not in self.profiles:
            raise Exception('Invaid profile name')
        else:
            self.__dict__.update(self.profiles[name])
            self.active_profile = name
            if name is 'global': self.active_profile = 'none'

settings = Settings()
