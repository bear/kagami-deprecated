kagami
======

Introduction
------------
In Japanese kagami means (according to the google searches I did) mirror.

The goal of kagami is to be more than a single protocol mirror, but right
now that is all it does.  

Currently it mirrors log file updates.

Listen to a set of defined log files for any update events and ship
them to a remote Logstash server using ZeroMQ as the transport.

Sample kagami.cfg

    { "debug": true,
      "address": "127.0.0.1",
      "client": true,
      "files":  ["/private/var/log/system.log"],
    }

Logstash Config requires a ZeroMQ Input item, e.g.

input {
  zeromq {
    type => "shipper-input"
    mode => "server"
    topology => "pushpull"
    address => "tcp://*:5556"
  }
}


Requires
--------
watchdog    http://pypi.python.org/pypi/watchdog
            http://github.com/gorakhargosh/watchdog

pyzmq       http://pypi.python.org/pypi/pyzmq
            https://github.com/zeromq/pyzmq

