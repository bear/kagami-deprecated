#!/usr/bin/env python

#
# :copyright: (c) 2012 by Mike Taylor
# :license: BSD 2-Clause
#

""" Establish log file/folder watchers and create a logstash
    style json item with the log line's meta data populated
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

from . import _ourName

log = logging.getLogger(_ourName)


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

            print len(lines), lines
            print '[%s]' % self.buffer
            if len(lines) > 0 and self.queue is not None:
                log.debug('sending %d lines to logstash' % len(lines))
                self.queue.put((event._src_path, lines))

def watcher(options, worker):
    """ Watchdog callback handler

        Loop thru the list of files to monitor and schedule via Watchdog
        an observer to do the work.
    """
    observer = Observer()
    watchers = {}

    for file in options['files']:
        if os.path.exists(file):
            watchers[file] = WatchFileEventHandler(file, options['events'])
            observer.schedule(watchers[file], watchers[file].path, recursive=False)
            log.info('Watcher established for %s' % file)
        else:
            log.error('File and/or path does not exist: %s' % file)

    observer.start()
    observer.join()

def watch(options, worker):
    """ Start as processes the file monitor and also the logstash zeromq emitter
    """
    return Process(name=worker['key'], target=watcher, args=(options, worker))
