import sys
import time
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
        self.reliability_calculator = ReliabilityCalculator()
        self.failure_counts = defaultdict(lambda: 0)
        self.node_uris = {}
        self.node_start_times = defaultdict(lambda: time.time())
        #self.failure_times = defaultdict(lambda: [])
        #TODO Same history size?
        self.replication_times_millis = defaultdict(lambda: deque(self.history_size *[(time.time(), 100)], maxlen=self.history_size))

    def register(self, node_id, usage, uri):
        _log.debug("Registering resource usage for node {}: {} with uri {}".format(node_id, usage, uri))
        if isinstance(uri, list):
            uri = uri[0]
        if not uri in self.node_uris.values():
            self.node_start_times[uri] = time.time()
            #self.failure_times[uri][0] = time.time()  # For reference
        self.node_uris[node_id] = uri
        if usage:
            self.usages[node_id].append(usage)

    def lost_node(self, node_id, uri):
        _log.debug("Registering lost node: {} - {}".format(node_id, uri))
        self.node_uris[node_id] = uri
        self.failure_counts[uri] += 1
        #self.failure_times[uri].append(time.time())

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

    def get_reliability(self, node_id, actor_type):
        uri = self.node_uris.get(node_id)
        fail_count = self.failure_counts[uri]
        start_time = self.node_start_times[uri]
        replication_time = self._average_replication_time(actor_type)
        return self.reliability_calculator.calculate_reliability(fail_count, start_time, replication_time)

    def _average_replication_time(self, actor_type):
        time = sum(x[1] for x in self.replication_times_millis[actor_type]) / self.history_size
        return time

    def update_replication_time(self, actor_type, replication_time):
        _log.info('New replication time: {}'.format(replication_time))
        self.replication_times_millis[actor_type].append((time.time(), replication_time))

    def sort_nodes_reliability(self, node_ids):
        """Sorts after number of failures"""
        _log.debug("Sorting nodes {} after reliability {}".format(node_ids, self.failure_counts))
        node_ids = [(node_id, self.failure_counts[self.node_uris.get(node_id)]) for node_id in node_ids]
        node_ids.sort(key=lambda x: x[1])
        return [x[0] for x in node_ids]

    def current_reliability(self, current_nodes, actor_type):
        _log.debug("Calculating reliability for nodes: {}".format(current_nodes))
        failure = 1
        for node_id in current_nodes:
            uri = self.node_uris.get(node_id)

            fail_count = self.failure_counts[uri]
            start_time = self.node_start_times[uri]
            replication_time = self._average_replication_time(actor_type)

            failure *= (1 - self.reliability_calculator.calculate_reliability(fail_count, start_time, replication_time))

        _log.debug("Reliability for nodes {} is {}".format(current_nodes, 1 - failure))
        return 1 - failure

    def update_node_failure(self, node_id, nbr_of_failures, uri):
        """ Simulates node failures """
        self.node_uris[node_id] = uri
        self.failure_counts[uri] += int(nbr_of_failures)