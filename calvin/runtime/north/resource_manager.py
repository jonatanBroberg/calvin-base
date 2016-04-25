import sys
import time
import operator
import socket
import re

from collections import defaultdict, deque
from calvin.runtime.north.reliability_calculator import ReliabilityCalculator

from calvin.utilities.calvinlogger import get_logger

_log = get_logger(__name__)

DEFAULT_HISTORY_SIZE = 20
DEFAULT_REPLICATION_HISTORY_SIZE = 5
DEFAULT_REPLICATION_TIME = 2000
DEFAULT_NODE_REALIABILITY = 0.8
LOST_NODE_TIME = 500
MAX_PREFERRED_USAGE = 80


class ResourceManager(object):
    def __init__(self, history_size=DEFAULT_HISTORY_SIZE):
        self.history_size = history_size
        self.usages = defaultdict(lambda: deque(maxlen=self.history_size))
        self.reliability_calculator = ReliabilityCalculator()
        self.node_uris = {}
        self.failure_info = defaultdict(lambda: [])                     #{node_id: [(time.time(), node_id)...}
        self.replication_times_millis = defaultdict(lambda: deque(maxlen=DEFAULT_REPLICATION_HISTORY_SIZE))
        self.test_sync = 2
        self._lost_nodes = set()

    def register_uri(self, node_id, uri):
        _log.debug("Registering uri: {} - {}".format(node_id, uri))
        if isinstance(uri, list):
            uri = uri[0]

        if uri:
            uri = uri.replace("calvinip://", "").replace("http://", "")
            addr = uri.split(":")[0]
            port = int(uri.split(":")[1])

            is_ip = re.match(r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$", addr)
            if not is_ip:
                addr = socket.gethostbyname(addr)

            self.node_uris[node_id] = "{}:{}".format(addr, port)

    def register(self, node_id, usage, uri):
        _log.debug("Registering resource usage for node {}: {} with uri {}".format(node_id, usage, uri))
        if isinstance(uri, list):
            uri = uri[0]

        if usage:
            self.usages[node_id].append(usage['cpu_percent'])

    def lost_node(self, node_id, uri):
        if node_id in self._lost_nodes:
            return
        self._lost_nodes.add(node_id)
        del self.usages[node_id]
        _log.debug("Registering lost node: {} - {}".format(node_id, uri))
        uri = uri.replace("calvinip://", "").replace("http://", "") if uri else uri
        self.node_uris[node_id] = uri
        self._add_failure_info(uri, [(time.time(), node_id)])

    def _average_usage(self, node_id):
        return sum(self.usages[node_id]) / max(len(self.usages[node_id]), 1)

    def least_busy(self):
        """Returns the id of the node with the lowest average CPU usage"""
        min_usage, least_busy = sys.maxint, None
        for node_id in self.usages.keys():
            average = self._average_usage(node_id)
            if average < min_usage:
                min_usage = average
                least_busy = node_id

        return least_busy

    def most_busy(self):
        """Returns the id of the node with the highest average CPU usage"""
        min_usage, most_busy = - sys.maxint, None
        for node_id in self.usages.keys():
            average = self._average_usage(node_id)
            if average > min_usage:
                min_usage = average
                most_busy = node_id

        return most_busy

    def get_reliability(self, node_id, actor_type):
        uri = self.node_uris.get(node_id)
        if uri:
            failure_info = self.failure_info[uri]
            replication_time = self._average_replication_time(actor_type)
            return self.reliability_calculator.calculate_reliability(failure_info, replication_time)
        else:
            return DEFAULT_NODE_REALIABILITY

    def get_preferred_nodes(self, nodes):
        preferred = []
        for node_id in nodes:
            if self._average_usage(node_id) < MAX_PREFERRED_USAGE:
                preferred.append(node_id)
        return preferred

    def _average_replication_time(self, actor_type):
        _log.debug("Getting replication time for type {} - {}".format(actor_type, self.replication_times_millis))
        if not self.replication_times_millis[actor_type]:
            return DEFAULT_REPLICATION_TIME
        times = self.replication_times_millis[actor_type]
        time = sum(x[1] for x in times) / max(len(times), 1)
        return time + LOST_NODE_TIME

    def _update_deque(self, new_values, old_values):
        for tup in new_values:
            if tup[0] > old_values[-1][0]:
                old_values.append(tup)

    def update_replication_time(self, actor_type, replication_time, timestamp, node_id=None):
        _log.info('New replication time: {} when handling lost node {}'.format(replication_time, node_id))
        if not self.replication_times_millis[actor_type]:
            self.replication_times_millis[actor_type].append((timestamp, replication_time))
        elif timestamp not in [x[0] for x in self.replication_times_millis[actor_type]]:
            self.replication_times_millis[actor_type].append((timestamp, replication_time))

    def sort_nodes_reliability(self, node_ids, actor_type):
        """Sorts after number of failures"""
        node_ids = [(node_id, self.get_reliability(node_id, actor_type)) for node_id in node_ids]
        node_ids.sort(key=lambda x: (x[1], x[0]), reverse=True)
        _log.debug("Sorting nodes {} after reliability {}".format([x[0] for x in node_ids], [x[1] for x in node_ids]))
        return [x[0] for x in node_ids]

    def current_reliability(self, current_nodes, actor_type):
        current_nodes = list(set(current_nodes))
        _log.debug("Calculating reliability for nodes: {}".format(current_nodes))
        failure = []
        for node_id in current_nodes:
            f = 1 - self.get_reliability(node_id, actor_type)
            _log.debug("Failure for {}: {}".format(node_id, f))
            failure.append(f)

        p = 1 - reduce(operator.mul, failure, 1)
        _log.debug("Reliability for nodes {} is {}".format(current_nodes, p))
        return p

    def update_node_failure(self, node_id, nbr_of_failures, uri):
        """ Simulates node failures """
        self.node_uris[node_id] = uri
        self._add_failure_info(uri, [(time.time(), node_id)])

    def _add_failure_info(self, uri, list_of_failures):
        old_info = self.failure_info[uri]
        for (time, node_id) in list_of_failures:
            if node_id not in [x[1] for x in old_info]:
                old_info.append((time, node_id))
        old_info = sorted(old_info, key=lambda x:x[0])

        while len(self.failure_info[uri]) > 4:
            self.failure_info[uri].pop(0)

    def sync_info(self, replication_times=None, failure_info=None, usages=None):
        if replication_times:
            self._sync_replication_times(replication_times)

        if failure_info:
            self._sync_failure_info(failure_info)

        if usages:
            self.sync_usages(usages)

        replication_times = {}
        for (actor_type, times) in self.replication_times_millis.iteritems():
            replication_times[actor_type] = [(x, y) for x, y in times]

        usages = {}
        for (node_id, usage_list) in self.usages.iteritems():
            usages[node_id] = [usage for usage in usage_list]

        return [replication_times, self.failure_info, usages]

    def _sync_replication_times(self, replication_times):
        """
        Sync the replication_times for each actor_type stored on another node.
        replication_times is a sent as a list but stored as a deque
        """
        _log.debug("Syncing replication times {} with new replication times {}".format(
            self.replication_times_millis, replication_times))
        for (actor_type, times) in replication_times.iteritems():
            sorted_times = sorted(times, key=lambda x:x[0])
            sorted_times = [(x,y) for x,y in sorted_times]
            if actor_type in self.replication_times_millis.keys() and len(self.replication_times_millis[actor_type]) > 0:
                self._update_deque(sorted_times, self.replication_times_millis[actor_type])
            else:
                for key, value in sorted_times:
                    self.replication_times_millis[actor_type].append((key, value))
        _log.debug("Replication times: {}".format(self.replication_times_millis))

    def _sync_failure_info(self, failure_info):
        _log.debug("Syncing failure_info {} with new failure_info {}".format(self.failure_info, failure_info))
        for (uri, info_list) in self.failure_info.iteritems():
            if uri in failure_info.keys():
                self._add_failure_info(uri, sorted(info_list, key=lambda x:x[0]))
        for (uri, info_list) in failure_info.iteritems():
            if uri not in self.failure_info.keys():
                self._add_failure_info(uri, info_list)

    def sync_usages(self, usages):
        """
        Sync the usages for each node_id stored on another node.
        usages is a dict of lists but stored as a dict with deques
        """
        _log.debug("\n\nSyncing usages {} with usages {}".format(self.usages, usages))
        for (node_id, usage_list) in usages.iteritems():
            if (not node_id in self.usages.keys() or len(usage_list) > self.usages[node_id]) and not node_id in self._lost_nodes:
                usage_deq = deque(maxlen=self.history_size)
                for u in usage_list:
                    usage_deq.append(u)
                self.usages[node_id] = usage_deq