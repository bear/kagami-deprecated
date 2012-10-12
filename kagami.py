#!/usr/bin/env python

""" Logstash Proxy

    Process local log file updates using watchdog's inotify
    events and send via zeromq any new lines to a remote
    logstash service

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

    Authors:
        bear    Mike Taylor <bear@code-bear.com>
"""

import os,sys
import json
import time
import types
import logging
import datetime

from Queue import Empty
from optparse import OptionParser
from logging.handlers import RotatingFileHandler
from multiprocessing import Process, Queue, get_logger

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

import zmq


_version_   = u'0.4.0'
_copyright_ = u'Copyright (c) 2012 Mike Taylor'
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
            echoFormatter = logging.Formatter('%(levelname)-7s %(processName)s[%(process)d]: %(message)s')
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

            if event is not None:
                try:
                    filename, lines = event
                    timestamp       = datetime.datetime.utcnow().isoformat()

                    log.debug('sending %s' % filename)
                    for line in lines:
                        payload = json.dumps({'@source': "file://{0}{1}".format('app1', filename),
                                              '@type': 'linux-syslog',
                                              '@tags': [],
                                              '@fields': {},
                                              '@timestamp': timestamp,
                                              '@source_host': 'app1',
                                              '@source_path': filename,
                                              '@message': line.strip(os.linesep),
                                             })
                        sender.send(payload)
                except:
                    log.error('Error during send() - exiting', exc_info=True)
                    break

        except Empty:
            time.sleep(1)

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

class WatchFileEventHandler(FileSystemEventHandler):
    def __init__(self, filename, queue=None):
        super(FileSystemEventHandler, self).__init__()
        self.queue    = queue
        self.filename = os.path.abspath(filename)
        self.file     = os.path.basename(self.filename)
        self.path     = os.path.dirname(self.filename)
        self.buffer   = ''

        if os.path.exists(self.filename):
            self.handle = open(self.filename, 'rb')
            self.handle.seek(0, os.SEEK_END)

    def on_any_event(self, event):
        if not event._is_directory and (os.path.basename(event._src_path) == self.file):
            log.debug('watched file event: [%s] %s' % (event._src_path, event._event_type))

            data  = self.handle.read()
            lines = data.split('\n')
            if len(self.buffer) > 0:
                lines[0] = '%s%s' % (self.buffer, lines[0])
            if lines[-1]:
                self.buffer = lines[-1]
                lines.pop()

            if self.queue is None:
                print len(lines), '[%s]' % self.buffer
                for line in lines:
                    print self.file, self.path, line
            else:
                self.queue.put((self.file, self.path, lines))


def watcher(options, events):
    observer = Observer()
    watchers = {}

    for file in options.files:
        if os.path.exists(file):
            watchers[file] = WatchFileEventHandler(file)
            observer.schedule(watchers[file], watchers[file].path, recursive=False)
        else:
            log.error('File and/or path does not exist: %s' % file)

    observer.start()
    observer.join()


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
        Process(name='receiver', target=zmqReceiver, args=(options, inbound)).start()

    if options.client:
        Process(name='sender', target=zmqSender, args=(options, outbound)).start()
        Process(name='watcher', target=watcher, args=(options, outbound)).start()
