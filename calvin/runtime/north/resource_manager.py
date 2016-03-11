import sys
import operator

from collections import defaultdict, deque
from calvin.runtime.north.reliability_calculator import ReliabilityCalculator

from calvin.utilities.calvinlogger import get_logger

_log = get_logger(__name__)

DEFAULT_HISTORY_SIZE = 5


class ResourceManager(object):
    def __init__(self, history_size=DEFAULT_HISTORY_SIZE):
        self.history_size = history_size
        self.usages = defaultdict(lambda: deque(maxlen=self.history_size))
        self.reliabilities = {}
        self.reliability_calculator = ReliabilityCalculator()
        self.failure_counts = defaultdict(lambda: 0)
        self.node_uris = {}

    def register(self, node_id, usage, uri):
        _log.debug("Registering resource usage for node {}: {} with uri {}".format(node_id, usage, uri))
        self.node_uris[node_id] = uri
        if usage:
            self.usages[node_id].append(usage)
        self.reliabilities[node_id] = 0.8

    def lost_node(self, node_id, uri):
        self.node_uris[node_id] = uri

        #_log.info("Lost node: {}/{}".format(node_id, uri))
        #for key in self.failure_counts:
        #    _log.info("Failure count: {}: {}".format(key, self.failure_counts[key]))
        self.failure_counts[uri] += 1

    def _average(self, node_id):
        return sum([usage['cpu_percent'] for usage in self.usages[node_id]]) / self.history_size

    def least_busy(self):
        """Returns the id of the node with the lowest average CPU usage"""
        min_usage, least_busy = sys.maxint, None
        for node_id in self.usages.keys():
            average = self._average(node_id)
            if average < min_usage:
                min_usage = average
                least_busy = node_id

        return least_busy

    def most_busy(self):
        """Returns the id of the node with the highest average CPU usage"""
        min_usage, most_busy = - sys.maxint, None
        for node_id in self.usages.keys():
            average = self._average(node_id)
            if average > min_usage:
                min_usage = average
                most_busy = node_id

        return most_busy

    def _update_reliability(self, node_id):
        self.reliabilities[node_id] = self.reliability_calculator.calculate_reliability(1.0, 10)

    def get_reliability(self, node_id):
        return self.reliabilities[node_id]

    def sort_nodes_reliability(self, node_ids):
        """Sorts after number of failures"""
        #_log.info("node uris: {}".format(self.node_uris))
        #_log.info("failure counts: {}".format(self.failure_counts))
        print self.failure_counts
        print self.node_uris
        print node_ids
        uris = [self.node_uris.get(node_id) for node_id in node_ids]
        print uris
        print [self.failure_counts.get(uri) for uri in uris]
        node_ids = [(node_id, self.failure_counts[self.node_uris.get(node_id)]) for node_id in node_ids]
        node_ids.sort(key=lambda x: x[1])
        node_ids = [x[0] for x in node_ids]
        return node_ids

    def current_reliability(self, current_nodes):
        current = 1
        for node_id, reliability in self.reliabilities.iteritems():
            if node_id in current_nodes:
                current *= (1 - reliability)
        if current == 1:
            return 0
        else:
            return 1 - current
