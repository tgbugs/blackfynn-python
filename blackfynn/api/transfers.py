# -*- coding: utf-8 -*-

import io
import os
import sys
import uuid
import time
import boto3
import threading
from botocore.client import Config
from boto3.s3.transfer import S3Transfer
from concurrent.futures import ThreadPoolExecutor, as_completed

# blackfynn
from blackfynn import settings
from blackfynn.api.base import APIBase
from blackfynn.models import TimeSeries, Collection, Dataset
from blackfynn.utils import log

# GLOBAL
UPLOADS = {}

def check_files(files):
    for f in files:
        if not os.path.exists(f):
            raise Exception("File {} not found.".format(f))


class ProgressPercentage(object):
    def __init__(self, filename, upload_session_id):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()
        self._progress = 0
        self._done = False
        self._errored = False

        if upload_session_id is None:
            upload_session_id = str(uuid.uuid4())
        self._session_id = upload_session_id

    def __call__(self, bytes_amount):
        global UPLOADS
        # To simplify we'll assume this is hooked up
        # to a single filename.
        if self._errored:
            return
        with self._lock:
            self._seen_so_far += bytes_amount
            self._done = self._seen_so_far >= self._size
            UPLOADS[self._session_id] = self

    @property
    def progress(self):
        return (self._seen_so_far / self._size)

    def set_error(self):
        self._errored = True


class UploadManager(object):
    def __init__(self):
        self.id = str(uuid.uuid4())
        # map of filename -> progress callback
        self._uploads = {}
        self._exit = False

    def init_file(self, filename):
        global UPLOADS
        # filename -> session
        uid = str(uuid.uuid4())
        self._uploads[filename] = uid
        # init to blank progress
        UPLOADS[uid] = ProgressPercentage(filename, None)
        return uid

    @property
    def uploads(self):
        return [(f, UPLOADS[uid]) for f, uid in self._uploads.items()]

    @property
    def done(self):
        return sum([fstat._done for _,fstat in self.uploads]) == len(self._uploads)

    def _print_progress(self, width=16):
        for f, fstat in self.uploads:

            if fstat._done:
                state = 'DONE'
            elif fstat._errored:
                state = 'ERRORED'
                self._exit = True
            elif fstat._seen_so_far>0:
                state = 'UPLOADING'
            else:
                state = 'WAITING'

            text = ' [ {bars}{dashes} ] {state:12s} {percent:05.1f}% {name}\n'.format(
                        bars = '#'*int(fstat.progress*width),
                        dashes = '-'*(width - int(fstat.progress*width)),
                        percent = fstat.progress*100,
                        name = os.path.basename(f),
                        state = state)

            sys.stdout.write('{}\r'.format(text))
            sys.stdout.flush()

        # move cursor to relative beginning
        sys.stdout.write("\033[F"*len(self.uploads))

    def display_progress(self):
        while True:
            self._print_progress()
            time.sleep(0.2)
            if self.done or self._exit: break
        self._print_progress()
        # move cursor to bottom
        sys.stdout.write('\n'*len(self.uploads))


def upload_file(
        file,
        s3_bucket,
        s3_keybase,
        region,
        access_key_id,
        secret_access_key,
        session_token,
        encryption_key_id,
        upload_session_id=None):

    # progress callback
    progress = ProgressPercentage(file, upload_session_id)
    UPLOADS[upload_session_id] = progress

    try:
        # account for dev connections
        resource_args = {}
        config_args = dict(signature_version='s3v4')
        if 'amazon' not in settings.s3_host.lower() and len(settings.s3_host)!=0:
            resource_args = dict(endpoint_url="http://{}:{}".format(settings.s3_host, settings.s3_port))
            config_args = dict(s3=dict(addressing_style='path'))

        # connect to s3
        session = boto3.session.Session()
        s3 = session.client('s3',
            region_name = region,
            aws_access_key_id = access_key_id,
            aws_secret_access_key = secret_access_key,
            aws_session_token = session_token,
            config = Config(**config_args),
            **resource_args
        )

        # s3 key
        s3_key = '{}/{}'.format(s3_keybase, os.path.basename(file))

        # upload file to s3
        s3.upload_file(
            Filename=file,
            Bucket=s3_bucket,
            Key=s3_key,
            Callback=progress,
            ExtraArgs=dict(
                ServerSideEncryption="aws:kms",
                SSEKMSKeyId=encryption_key_id
            ))

        return s3_key

    except Exception as e:
        log.debug(e)
        progress.set_error()
        raise e


class IOAPI(APIBase):
    """
    Input/Output interface.
    """
    name = 'io'

    def upload_files(self, destination, files, dataset=None, append=False, display_progress=False):
        group_id = str(uuid.uuid4())
        if isinstance(destination, Dataset):
            # uploading into dataset
            destination_id = None
            dataset_id = self._get_id(destination)
        elif destination is None and dataset is not None:
            # uploading into dataset
            destination_id = None
            dataset_id = self._get_id(dataset)
        elif isinstance(destination, Collection):
            # uploading into collection
            destination_id = self._get_id(destination)
            dataset_id = self._get_id(destination.dataset)
        elif append and isinstance(destination, TimeSeries):
            # uploading into timeseries package must be an append
            destination_id = self._get_id(destination)
            dataset_id = self._get_id(destination.dataset)
        elif isinstance(destination, basestring):
            # assume ID is for collection
            if dataset is None:
                raise Exception("Must also supply dataset when specifying destination by ID")
            destination_id = destination
            dataset_id = self.get_id(dataset)
        else:
            raise Exception("Cannot upload to destination of type {}".format(type(destination)))

        # check input files
        check_files(files)

        # sanity check dataset
        try:
            ds = self.session.datasets.get(dataset_id)
        except:
            raise Exception("dataset does not exist")

        if destination_id is not None:
            try:
                destination = self.session.packages.get(destination_id)
            except:
                raise Exception("destination does not exist")

            # check type for appends
            if append:
                if not isinstance(destination, TimeSeries):
                    raise Exception("Append destination must be TimeSeries package.")
            else:
                if not isinstance(destination, Collection):
                    raise Exception("Upload destination must be Collection or Dataset.")


        # get upload credentials
        resp = self.session.security.get_upload_credentials(dataset_id)
        creds = resp['tempCredentials']
        s3_bucket = resp['s3Bucket']
        s3_keybase = '{}/{}'.format(resp['s3Key'], group_id)
        region = creds['region']
        access_key_id = creds['accessKey']
        secret_access_key = creds['secretKey']
        session_token = creds['sessionToken']
        encryption_key_id = resp['encryptionKeyId']

        # upload session manager
        upload_session = UploadManager()

        # parallel upload to s3
        results = {}
        with ThreadPoolExecutor(max_workers=min(len(files),settings.max_upload_workers)) as e:
            futures = {
                e.submit(
                    fn = upload_file,
                    file = file,
                    s3_bucket = s3_bucket,
                    s3_keybase = s3_keybase,
                    region = region,
                    access_key_id = access_key_id,
                    secret_access_key = secret_access_key,
                    session_token = session_token,
                    encryption_key_id = encryption_key_id,
                    upload_session_id = upload_session.init_file(file)
                ): file
                for file in files
            }
            # thread for displaying progress
            if display_progress:
                e.submit(upload_session.display_progress())
            # retrieve results
            for future in as_completed(futures):
                fname = futures[future]
                results[fname] = future.result()
                if future.exception() is not None:
                    raise future.exception()

        # trigger ETL import
        return self.set_upload_complete(group_id, dataset_id, destination_id, append)

    def set_upload_complete(self, group_id, dataset_id, destination_id, append=False):
        params = dict(
            append = append,
            datasetId = dataset_id,
            groupId = group_id,
        )
        if destination_id is not None:
            params['destinationId'] = destination_id

        return self._post(
            endpoint = self._uri('/files/upload/complete/{group_id}', group_id=group_id),
            params = params)
