import urllib2
import json
import pprint
import collectd

states = {
    "Initializing": 0,
    "Unknown": 1,
    "PreReplica": 2,
    "CatchingUp": 3,
    "Clone": 4,
    "Slave": 5,
    "PreMaster": 6,
    "Master": 7,
    "Manager": 8,
    "ShuttingDown": 9,
    "Shutdown": 10
}


class EventstoreMonitor(object):

    def __init__(self):
        self.pp = pprint.PrettyPrinter(indent=4)

    def configure(self, a):
        for child in a.children:
            if child.key == "gossip_uri":
                self.gossip_uri = child.values[0]

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

    def read(self):
        response = urllib2.urlopen(self.gossip_uri)
        data = json.load(response)
        num_masters = 0
        num_nodes = 0
        num_alive = 0
        my_ip = data["serverIp"]
        for m in data["members"]:
            if m["internalHttpIp"] == my_ip:
                self.submit_gauge("node", "state", states[m["state"]])
                self.submit_counter(
                    "node",
                    "writer-checkpoint",
                    m["writerCheckpoint"])
                self.submit_counter("node", "epoch-number", m["epochNumber"])
                self.submit_counter(
                    "node",
                    "chaser-checkpoint",
                    m["chaserCheckpoint"])
                self.submit_counter("node", "epoch-position", m["epochPosition"])
            if m["state"] == "Master":
                num_masters = num_masters + 1
            if m["isAlive"]:
                num_alive = num_alive + 1
            num_nodes = num_nodes + 1
        self.submit_gauge("cluster", "master-nodes", num_masters)
        self.submit_gauge("cluster", "live-nodes", num_alive)
        self.submit_gauge("cluster", "nodes", num_nodes)


monitor = EventstoreMonitor()
collectd.register_config(monitor.configure)
# collectd.register_init(monitor.init)
collectd.register_read(monitor.read)

