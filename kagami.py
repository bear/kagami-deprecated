#!/usr/bin/env python

#
# :copyright: (c) 2012 by Mike Taylor
# :license: BSD 2-Clause
#

""" Logstash Proxy

    Process local log file updates using watchdog's inotify
    events and send via zeromq any new lines to a remote
    logstash service

    Assumes Python v2.7+

    Usage
        -c --config         Configuration file (json format)
                            default: None
        -d --debug          Turn on debug logging
                            default: False
        -l --logpath        Path where the log file output is written
                            default: None
        -b --background     Fork to a daemon process
                            default: False

    Authors:
        bear    Mike Taylor <bear@code-bear.com>
"""

import signal
from kagami import initOptions, initLogs, configWorkers
from kagami.logstash import emit
from kagami.watchers import watch

from pprint import pprint

def handleSIGTERM(signum, frame):
    raise KeyboardInterrupt

signal.signal(signal.SIGTERM, handleSIGTERM)



_defaultOptions = { 'config':     ('-c', '--config',     None,  'Configuration file'),
                    'debug':      ('-d', '--debug',      True,  'Enable Debug'),
                    'background': ('-b', '--background', False, 'daemonize ourselves'),
                    'logpath':    ('-l', '--logpath',    None,  'Path where log file is to be written'),
                  }

if __name__ == '__main__':
    options = initOptions(params=_defaultOptions)
    initLogs(options)
 
    workers = configWorkers(options)

    for key in workers:
        worker     = workers[key]
        workerType = worker['type']
        if workerType == 'watch':
            worker['pid'] = watch(options, worker)
        elif workerType == 'logstash':
            worker['pid'] = emit(options, worker)            
        else:
            log.error('Unknown type %s - worker not started' % workerType)
  
        if worker['pid'] is not None:
            worker['pid'].start()
            worker['pid'].join()
