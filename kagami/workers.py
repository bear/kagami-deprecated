#!/usr/bin/env python

#
# :copyright: (c) 2012 by Mike Taylor
# :license: BSD 2-Clause
#

import logging

from multiprocessing import Queue

from . import _ourName


log = logging.getLogger(_ourName)


class Worker():
    def __init__(self, config):
        self.config = config

        if 'type' not in config:
            raise KagamiError('The worker definition must include a worker "type"')
        if 'id' not in config:
            raise KagamiError('The worker definition must include a worker "id"')

        self.type = config['type'].lower()
        self.name = config['id'].lower()
        self.events = Queue()

class WorkerManager():
    def __init__(self, log=None):
        self.workers = {}

    def configure(self, config):
        if config is not None:
            for item in config:
                worker = Worker(item)

                if worker.name in self.workers:
                    log.error('Worker IDs must be unique: %s already present' % worker.name)
                    worker = None
                    continue
                if worker.type not in ('watch', 'logstash'):
                    log.error('Worker type %s is not a valid type' % worker.type)
                    worker = None
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
