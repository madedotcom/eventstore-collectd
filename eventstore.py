import urllib2
import json
import pprint
from collections import defaultdict, namedtuple

stat = namedtuple("name", "type")


class EventstoreMonitor(object):

    def __init__(self):
        self.pp = pprint.PrettyPrinter(indent=4)
        self.include_all_queues = False

    def configure(self, a):
        for child in a.children:
            if child.key == "stats_uri":
                self.stats_uri = child.values[0]
            if child.key == "include_all_queues":
                self.include_all_queues = child.values[0]


    def submit_counter(self, instance, metric, value):
        val = collectd.Values(plugin='eventstore')
        val.type = 'counter'
        val.type_instance = instance+"/"+metric
        val.values = [value]
        val.dispatch()


    def submit_gauge(self, instance, metric, value):
        val = collectd.Values(plugin='eventstore')
        val.type = 'gauge'
        val.type_instance = instance+"/"+metric
        val.values = [value]
        val.dispatch()


    def send_queue(self, name, queue):
        self.submit_gauge("queue-"+name, "length", queue["length"])
        self.submit_counter("queue-"+name, "total-processed", queue["totalProcessed"])
        self.submit_gauge("queue-"+name, "processing-time", queue["avgProcessingTime"])

    def send_proc(self, data):
        self.submit_gauge('threads', 'count', data['threadsCount'])
        self.submit_gauge('error', 'count', data['thrownExceptionsRate'])

        self.submit_counter('disk', 'read-bytes', data["diskIo"]['readBytes'])
        self.submit_counter('disk', 'write-bytes', data["diskIo"]['writtenBytes'])
        self.submit_counter('disk', 'read-ops', data["diskIo"]['readOps'])
        self.submit_counter('disk', 'write-ops', data["diskIo"]['writeOps'])

        self.submit_gauge('tcp', 'connections', data['tcp']['connections'])
        self.submit_counter(
            'tcp',
            'bytes-received',
            data['tcp']['receivedBytesTotal'])
        self.submit_counter(
            'tcp',
            'bytes-sent',
            data['tcp']['sentBytesTotal'])

    def empty_queue(self):
        return (0, {
            "length": 0,
            "totalProcessed": 0,
            "avgProcessingTime": 0
        })

    def send_queues(self, queues):
        groups = defaultdict(self.empty_queue)
        for queuename, queue in queues.iteritems():
           if queue["groupName"]:
                count, group = groups[queue["groupName"]]
                groups[queue["groupName"]] = (count + 1, {
                    "length": queue["length"] + group["length"],
                    "totalProcessed": queue["totalItemsProcessed"] + group["totalProcessed"],
                    "avgProcessingTime": (
                        (group["avgProcessingTime"] * count)
                        + queue["avgProcessingTime"]) / (1 + count)
                })
                if self.include_all_queues:
                    self.send_queue(queue["queueName"], {
                        "length": queue["length"],
                        "totalProcessed": queue["totalItemsProcessed"],
                        "avgProcessingTime": queue["avgProcessingTime"]
                    })
           else:
                self.send_queue(queue["queueName"], {
                    "length": queue["length"],
                    "totalProcessed": queue["totalItemsProcessed"],
                    "avgProcessingTime": queue["avgProcessingTime"]
                })

        for group_name, (count, group) in groups.iteritems():
            self.send_queue(group_name, group)

    def send_writer(self, writer):
        self.submit_gauge('writer', 'flush-size', writer["lastFlushSize"])
        self.submit_gauge('writer', 'flush-delay', writer["lastFlushDelayMs"])
        self.submit_gauge('writer', 'queue-length', writer["queuedFlushMessages"])

    def read(self):
        response = urllib2.urlopen(self.stats_uri)
        data = json.load(response)
        self.send_proc(data["proc"])
        self.send_queues(data["es"]["queue"])

monitor = EventstoreMonitor()

import collectd

collectd.register_config(monitor.configure)
# collectd.register_init(monitor.init)
collectd.register_read(monitor.read)
