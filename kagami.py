#!/usr/bin/env python

""" Logstash Proxy

    :copyright: (c) 2012 by Mike Taylor
    :license: BSD 2-Clause

    Assumes Python v2.7+

    Usage
        -c --config         Configuration file (json format)
                            default: None
        -m --master         Start as the master
           --client         Start as a client
        -d --debug          Turn on debug logging
                            default: False
        -l --logpath        Path where the log file output is written
                            default: None
        -b --background     Fork to a daemon process
                            default: False

    Sample Configuration file


    Authors:
        bear    Mike Taylor <bear@code-bear.com>
"""

import os
import json
import time
import glob
import stat
import types
import errno
import logging
import datetime

from Queue import Empty
from optparse import OptionParser
from logging.handlers import RotatingFileHandler
from multiprocessing import Process, Queue, get_logger

import zmq

_version_   = u'0.4.0'
_copyright_ = u'Copyright (c) 2009 - 2012 Mike Taylor'
_license_   = u'BSD 2-Clause'

log      = get_logger()
outbound = Queue()
inbound  = Queue()
_ourPath = os.getcwd()
_ourName = os.path.splitext(os.path.basename(sys.argv[0]))[0]

LOGSTASH_PORT = 5556

# establish baseline logging with echo-to-screen handler
_echoHandler   = logging.StreamHandler()
_echoFormatter = logging.Formatter('%(levelname)-7s %(message)s')
_echoHandler.setFormatter(_echoFormatter)

log.addHandler(_echoHandler)
log.setLevel(logging.INFO)


def loadConfig(filename):
    result = {}
    if os.path.isfile(filename):
        try:
            result = json.loads(' '.join(open(filename, 'r').readlines()))
        except:
            log.error('error during loading of config file [%s]' % filename, exc_info=True)
    return result

def initOptions(defaults=None, params=None):
    """Parse command line parameters and populate the options object.
    """
    parser = OptionParser()

    defaultOptions = { 'config':  ('-c', '--config',  '',    'Configuration file'),
                       'debug':   ('-d', '--debug',   False, 'Enable Debug'),
                       'logpath': ('-l', '--logpath', '',    'Path where log file is to be written'),
                       'verbose': ('-v', '--verbose', False, 'show extra output from remote commands'),
                     }

    if params is not None:
        for key in params:
            defaultOptions[key] = params[key]

    if defaults is not None:
        for key in defaults:
            defaultOptions[key] = defaultOptions[key][0:2] + (defaults[key],) + defaultOptions[key][3:]

    for key in defaultOptions:
        items = defaultOptions[key]

        (shortCmd, longCmd, defaultValue, helpText) = items

        if type(defaultValue) is types.BooleanType:
            parser.add_option(shortCmd, longCmd, dest=key, action='store_true', default=defaultValue, help=helpText)
        else:
            parser.add_option(shortCmd, longCmd, dest=key, default=defaultValue, help=helpText)

    (options, args) = parser.parse_args()
    options.args    = args
    options.appPath = _ourPath

    if options.config is None:
        s = os.path.join(_ourPath, '%s.cfg' % _ourName)
        if os.path.isfile(s):
            options.config = s

    if options.config is not None:
        options.config = os.path.abspath(options.config)

        if not os.path.isfile(options.config):
            options.config = os.path.join(_ourPath, '%s.cfg' % options.config)

        if not os.path.isfile(options.config):
            options.config = os.path.abspath(os.path.join(_ourPath, '%s.cfg' % _ourName))

        jsonConfig = loadConfig(options.config)

        for key in jsonConfig:
            setattr(options, key, jsonConfig[key])

    if options.logpath is not None:
        options.logpath = os.path.abspath(options.logpath)

        if os.path.isdir(options.logpath):
            options.logfile = os.path.join(options.logpath, '%s.log'% _ourName)
        else:
            options.logfile = None

    if 'background' not in defaultOptions:
        options.background = False

    return options

def initLogs(options, chatty=True, loglevel=logging.INFO):

    # clear baseline echo-to-screen handler before setting up desired handler
    if _echoHandler is not None:
        log.removeHandler(_echoHandler)

    if options.logpath is not None:
        fileHandler   = RotatingFileHandler(os.path.join(options.logpath, '%s.log' % _ourName), maxBytes=1000000, backupCount=99)
        fileFormatter = logging.Formatter('%(asctime)s %(levelname)-7s %(processName)s: %(message)s')

        fileHandler.setFormatter(fileFormatter)

        log.addHandler(fileHandler)
        log.fileHandler = fileHandler

    if not options.background:
        echoHandler = logging.StreamHandler()

        if chatty:
            echoFormatter = logging.Formatter('%(levelname)-7s %(processName)s: %(message)s')
        else:
            echoFormatter = logging.Formatter('%(levelname)-7s %(message)s')

        echoHandler.setFormatter(echoFormatter)

        log.addHandler(echoHandler)
        log.info('echoing')

    if options.debug:
        log.setLevel(logging.DEBUG)
        log.info('debug level is on')
    else:
        log.setLevel(loglevel)


# hacked and borrowed from Beaver

class Watcher(object):
    def __init__(self, files, outbound, tail_lines=0):
        self.files_map = {}
        self.outbound  = outbound
        self.files     = files

        self.update_files()
        # The first time we run the script we move all file markers at EOF.
        # In case of files created afterwards we don't do this.
        for id, file in self.files_map.iteritems():
            file.seek(os.path.getsize(file.name))  # EOF
            if tail_lines:
                lines = self.tail(file.name, tail_lines)
                if lines:
                    self.outbound.put((file.name, lines))

    def __del__(self):
        self.close()

    def loop(self, interval=0.1, async=False):
        """Start the loop.
        If async is True make one loop then return.
        """
        while 1:
            self.update_files()
            for fid, file in list(self.files_map.iteritems()):
                self.readfile(file)
            if async:
                return
            time.sleep(interval)

    @staticmethod
    def tail(fname, window):
        """Read last N lines from file fname."""
        try:
            f = open(fname, 'r')
        except IOError, err:
            if err.errno == errno.ENOENT:
                return []
            else:
                raise
        else:
            BUFSIZ = 1024
            f.seek(0, os.SEEK_END)
            fsize = f.tell()
            block = -1
            data = ""
            exit = False
            while not exit:
                step = (block * BUFSIZ)
                if abs(step) >= fsize:
                    f.seek(0)
                    exit = True
                else:
                    f.seek(step, os.SEEK_END)
                data = f.read().strip()
                if data.count('\n') >= window:
                    break
                else:
                    block -= 1
            return data.splitlines()[-window:]

    def update_files(self):
        ls = []
        files = []
        if len(self.files) > 0:
            for name in self.files:
                files.extend([os.path.realpath(globbed) for globbed in glob.glob(name)])

        for absname in files:
            try:
                st = os.stat(absname)
            except EnvironmentError, err:
                if err.errno != errno.ENOENT:
                    raise
            else:
                if not stat.S_ISREG(st.st_mode):
                    continue
                fid = self.get_file_id(st)
                ls.append((fid, absname))

        # check existent files
        for fid, file in list(self.files_map.iteritems()):
            try:
                st = os.stat(file.name)
            except EnvironmentError, err:
                if err.errno == errno.ENOENT:
                    self.unwatch(file, fid)
                else:
                    raise
            else:
                if fid != self.get_file_id(st):
                    # same name but different file (rotation); reload it.
                    self.unwatch(file, fid)
                    self.watch(file.name)

        # add new ones
        for fid, fname in ls:
            if fid not in self.files_map:
                self.watch(fname)

    def readfile(self, file):
        lines = file.readlines()
        if lines:
            self.outbound.put((file.name, lines))

    def watch(self, fname):
        try:
            file = open(fname, "r")
            fid = self.get_file_id(os.stat(fname))
        except EnvironmentError, err:
            if err.errno != errno.ENOENT:
                raise
        else:
            log.info("[{0}] - watching logfile {1}".format(fid, fname))
            self.files_map[fid] = file

    def unwatch(self, file, fid):
        # file no longer exists; if it has been renamed
        # try to read it for the last time in case the
        # log rotator has written something in it.
        lines = self.readfile(file)
        log.info("[{0}] - un-watching logfile {1}".format(fid, file.name))
        del self.files_map[fid]
        if lines:
            self.outbound.put((file.name, lines))

    @staticmethod
    def get_file_id(st):
        return "%xg%x" % (st.st_dev, st.st_ino)

    def close(self):
        for id, file in self.files_map.iteritems():
            file.close()
        self.files_map.clear()

def zmqSender(options, events):
    log.info('starting sender')

    context = zmq.Context()
    sender  = context.socket(zmq.PUSH)
    address = options.address

    if ':' not in address:
        address = '%s:%s' % (address, LOGSTASH_PORT)

    log.debug('binding to tcp://%s' % address)

    sender.connect('tcp://%s' % address)

    while True:
        try:
            event = events.get(False)
        except Empty:
            event = None

        if event is not None:
            try:
                filename, lines = event
                timestamp = datetime.datetime.utcnow().isoformat()
#                {"@source":"file://li511-207/var/log/syslog",
#                 "@type":"linux-syslog",
#                 "@tags":[],
#                 "@fields":{},
#                 "@timestamp":"2012-10-10T07:15:01.881000Z",
#                 "@source_host":"li511-207",
#                 "@source_path":"/var/log/syslog",
#                 "@message":"Oct 10 07:15:01 li511-207 CRON[11531]: (munin) CMD (if [ -x /usr/bin/munin-cron ]; then /usr/bin/munin-cron; fi)"}

                log.debug('sending %s' % filename)
                for line in lines:
                    payload = json.dumps({ '@source': "file://{0}{1}".format('app1', filename),
                        '@type': 'linux-syslog',
                        '@tags': [],
                        '@fields': {},
                        '@timestamp': timestamp,
                        '@source_host': 'app1',
                        '@source_path': filename,
                        '@message': line.strip(os.linesep),
                        })

                    sender.send(payload)
#                    log.debug(sender.recv())

            except:
                log.error('Error during send() - exiting', exc_info=True)
                break

    log.info('done')

def zmqReceiver(options, events):
    log.info("starting receiver")

    zmqAddress = options.address

    if ':' not in zmqAddress:
        zmqAddress = '%s:%s' % (zmqAddress, LOGSTASH_PORT)

    log.debug('binding to tcp://%s' % zmqAddress)

    context = zmq.Context()
    server  = context.socket(zmq.PULL)

    server.bind('tcp://%s' % zmqAddress)

    while True:
        try:
            request = server.recv()
            log.debug('received [%s]' % request)
        except:
            log.error('error raised during recv()', exc_info=True)
            break


_defaultOptions = { 'config':      ('-c', '--config',     None,  'Configuration file'),
                    'debug':       ('-d', '--debug',      True,  'Enable Debug'),
                    'background':  ('-b', '--background', False, 'daemonize ourselves'),
                    'logpath':     ('-l', '--logpath',    None,  'Path where log file is to be written'),
                    'master':      ('-m', '--master',     False, 'Receive inbound JSON'),
                    'client':      ('',   '--client',     False, 'Send JSON to master'),
                    'address':     ('',   '--address',    None,  'Address to bind to'),
                  }


if __name__ == '__main__':
    options = initOptions(params=_defaultOptions)
    initLogs(options)

    log.info('Starting')

    if options.master:
        Process(name='rp-master', target=zmqReceiver, args=(options, inbound)).start()

    if options.client:
        Process(name='rp-client', target=zmqSender, args=(options, outbound)).start()

    if options.client:
        w = Watcher(options.files, outbound)
        w.loop()