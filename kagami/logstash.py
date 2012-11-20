#!/usr/bin/env python

#
# :copyright: (c) 2012 by Mike Taylor
# :license: BSD 2-Clause
#

"""
"""

import os
import json
import time
import logging
import datetime

from Queue import Empty
from multiprocessing import Process, Queue

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

import zmq

from . import _ourName

LOGSTASH_PORT = 5556

log = logging.getLogger(_ourName)

def logstashPayload(host, filename, logtype, data, tags=[], fields={}):
    result = json.dumps({'@source': "file://{0}{1}".format(host, filename),
                         '@type': logtype,
                         '@tags': tags,
                         '@fields': fields,
                         '@timestamp': datetime.datetime.utcnow().isoformat(),
                         '@source_host': host,
                         '@source_path': filename,
                         '@message': data,
                        })


def logstashEmitter(options, worker):
    log.info('starting logstash emitter')

    context = zmq.Context()
    sender  = context.socket(zmq.PUSH)
    events  = worker['events']
    address = worker['address']

    if ':' not in address:
        address = '%s:%s' % (address, LOGSTASH_PORT)

    log.debug('binding to tcp://%s' % address)

    sender.connect('tcp://%s' % address)

    while True:
        try:
            event = events.get(False)

            if event is not None:
                try:
                    sender.send(event)
                except:
                    log.error('Error during send() - exiting', exc_info=True)
                    break

        except Empty:
            time.sleep(1)

    log.info('done')


def emit(options, worker):
    """ Start the logstash zeromq emitter
    """
    return Process(name=worker['key'], target=logstashEmitter, args=(options, worker))
