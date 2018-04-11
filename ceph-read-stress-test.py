#! /usr/bin/env python


import os
import sys
import logging
import time
import random
import requests


logger = None
SYSTEM = 'STRESSING-CEPH'
COMPONENT = 'stressor'


FILENAME = '/test/aster_ged_file_list.txt'


# Standard logging filter for using Mesos
class LoggingFilter(logging.Filter):
    def __init__(self, system='', component=''):
        super(LoggingFilter, self).__init__()

        self.system = system
        self.component = component

    def filter(self, record):
        record.system = self.system
        record.component = self.component

        return True


# Standard logging formatter with special execption formatting
class ExceptionFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None):
        std_fmt = ('%(asctime)s.%(msecs)03d'
                   ' %(levelname)-8s'
                   ' %(system)s'
                   ' %(component)s'
                   ' %(message)s')
        std_datefmt = '%Y-%m-%dT%H:%M:%S'

        if fmt is not None:
            std_fmt = fmt

        if datefmt is not None:
            std_datefmt = datefmt

        super(ExceptionFormatter, self).__init__(fmt=std_fmt,
                                                 datefmt=std_datefmt)

    def formatException(self, exc_info):
        result = super(ExceptionFormatter, self).formatException(exc_info)
        return repr(result)

    def format(self, record):
        s = super(ExceptionFormatter, self).format(record)
        if record.exc_text:
            s = s.replace('\n', ' ')
            s = s.replace('\\n', ' ')
        return s


# Configure the message logging components
def setup_logging():

    global logger

    # Setup the logging level
    logging_level = logging.INFO

    handler = logging.StreamHandler(sys.stdout)
    msg_formatter = ExceptionFormatter()
    msg_filter = LoggingFilter(SYSTEM, COMPONENT)

    handler.setFormatter(msg_formatter)
    handler.addFilter(msg_filter)

    logger = logging.getLogger()
    logger.setLevel(logging_level)
    logger.addHandler(handler)


def get_env_var(variable, default):
    result = os.environ.get(variable, default)
    if result == None:
        raise RuntimeError('You must specify {} in the environment'
                           .format(variable))
    return result


class Web(object):
    '''
    Description:
        Provides methods for interfacing with web resources.
    '''

    @staticmethod
    def http_transfer_file(download_url, destination_file, headers=None):
        '''
        Description:
            Using http transfer a file from a source location to a destination
            file on the localhost.
        Returns:
            status_code - One of the following
                        - 200, requests.codes['ok']
                        - 404, requests.codes['not_found']:
                        - 503, requests.codes['service_unavailable']:
        Notes:
            If a 503 is returned, the logged exception should be reviewed to
            determine the real cause of the error.
        '''

        logger = logging.getLogger(__name__)

        logger.info(download_url)

        session = requests.Session()

        session.mount('http://', requests.adapters.HTTPAdapter(max_retries=3))
        session.mount('https://', requests.adapters.HTTPAdapter(max_retries=3))

        status_code = requests.codes['ok']
        req = None
        try:
            req = session.get(url=download_url, timeout=300.0,
                              headers=headers)

            if not req.ok:
                logger.error('HTTP - Transfer of [{0}] - FAILED'
                             .format(download_url))
                # The raise_for_status gets caught by this try's except
                # block
                req.raise_for_status()

            # Write the downloaded data to the destination file
            with open(destination_file, 'wb') as local_fd:
                local_fd.write(req.content)

            # Break the looping
            done = True
            logger.info('HTTP - Transfer Complete')

            hard_size = 44042064
            content_size = int(req.headers['content-length'])
            file_size = int(os.stat(destination_file).st_size)

            set_cookie = req.headers['Set-Cookie']

            logger.info('Set-Cookie = [{}], content_size = {}, file_size = {}'
                        .format(set_cookie, content_size, file_size))

            if content_size != file_size or content_size != hard_size:
                raise Exception('content_size = {}, file_size = {}'
                                .format(content_size, file_size))

        finally:
            if req is not None:
                req.close()

        return status_code


def main():

    setup_logging()

    object_store_url = str(get_env_var('OBJECT_STORE_URL', None))
    sleep_range_min = int(get_env_var('SLEEP_RANGE_MIN', 1))
    sleep_range_max = int(get_env_var('SLEEP_RANGE_MAX', 5))
    data_range_min = int(get_env_var('DATA_RANGE_MIN', 0))
    data_range_max = int(get_env_var('DATA_RANGE_MAX', 24872))
    # We have 24873 items in our list so 0-24872 index range

    os.chdir('/mnt/mesos/sandbox')

    data = list()

    with open(FILENAME, 'r') as fd:
        data = [x.strip() for x in fd]

    while True:
        random.seed(time.gmtime())

        index = random.randint(data_range_min, data_range_max)
        sleep_seconds = random.randint(sleep_range_min, sleep_range_max)

        # Add the bucket and filename to the url
        url_path = ''.join([object_store_url, '/AG100.003/', data[index]])

        status_code = Web.http_transfer_file(url_path, data[index])

        if status_code != requests.codes['ok']:
            if status_code != requests.codes['not_found']:
                raise Exception('HTTP - Transfer Failed')

        time.sleep(sleep_seconds)

        os.unlink(data[index])


if __name__ == '__main__':
    main()
