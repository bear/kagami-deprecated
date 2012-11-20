#!/usr/bin/env python

#
# :copyright: (c) 2012 by Mike Taylor
# :license: BSD 2-Clause
#

_version_   = u'0.5.0'
_copyright_ = u'Copyright (c) 2012 Mike Taylor'
_license_   = u'BSD 2-Clause'
__author__  = 'bear (Mike Taylor) <bear@code-bear.com>'

import os
import sys
import json
import types
import logging

from optparse import OptionParser
from logging.handlers import RotatingFileHandler
from multiprocessing import Process, Queue


_ourPath = os.getcwd()
_ourName = os.path.splitext(os.path.basename(sys.argv[0]))[0]
log      = logging.getLogger(_ourName)


class KagamiError(Exception):
    def __init__(self, *args):
        self.args = [p for p in args]


def loadConfig(filename):
    """ Read, parse and return given config file
    """
    result = {}
    if os.path.isfile(filename):
        try:
            result = json.loads(' '.join(open(filename, 'r').readlines()))
        except:
            print 'error during config file parsing'
            log.error('error during loading of config file [%s]' % filename, exc_info=True)
    return result

def initOptions(defaults=None, params=None):
    """ Parse command line parameters and populate the options object
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
    """ Initialize logging according to desired options
    """
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



class Worker(self):
    def __init__(self, config, options):
        self.config = config

        if 'type' not in config:
            raise KagamiError('The worker definition must include a worker "type"')
        if 'id' not in config:
            raise KagamiError('The worker definition must include a worker "id"')

        self.type = config['type'].lower()
        self.name = config['id'].lower()


def configWorkers(options):
    workers = {}
    if options.workers is not None:
        for item in options.workers:
            worker = Worker(item, options)
            if 'type' not in worker:
                log.error('Worker definition must include a type item')
                continue
            worker['type'] = worker['type'].lower()
            if 'id' not in worker:
                log.error('Worker definition must include an ID item')
                continue
            worker['key'] = worker['id'].lower()
            if worker['key'] in options.workers:
                log.error('Worker IDs must be unique: %s already present' % worker['id'])
                continue
            if worker['type'] not in ('watch', 'logstash'):
                log.error('Worker type %s is not a valid type' % worker['type'])
                continue
            worker['events']       = Queue()
            worker['sinks']        = []
            worker['processes']    = []
            workers[worker['key']] = worker

        for key in workers:
            worker = workers[key]
            if 'output' in worker:
                for target in worker['output']:
                    target = target.lower()
                    if target in workers:
                        worker['sinks'].append(workers[target]['events'])

    return workers
