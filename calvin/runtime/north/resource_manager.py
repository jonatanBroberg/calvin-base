import sys

from collections import defaultdict, deque

from calvin.utilities.calvinlogger import get_logger

_log = get_logger(__name__)

DEFAULT_HISTORY_SIZE = 5


class ResourceManager(object):
    def __init__(self, history_size=DEFAULT_HISTORY_SIZE):
        self.history_size = history_size
        self.usages = defaultdict(lambda: deque(maxlen=self.history_size))

    def register(self, node_id, usage):
        _log.debug("Registering resource usage for node {}: {}".format(node_id, usage))
        self.usages[node_id].append(usage)

    def least_busy(self):
        """Returns the id of the node with the lowest average CPU usage"""
        min_usage, least_busy = sys.maxint, None
        for node_id in self.usages.keys():
            average = sum(self.usages[node_id]) / self.history_size
            if average < min_usage:
                min_usage = average
                least_busy = node_id

        return least_busy

    def most_busy(self):
        """Returns the id of the node with the highest average CPU usage"""
        min_usage, most_busy = - sys.maxint, None
        for node_id in self.usages.keys():
            average = sum(self.usages[node_id]) / self.history_size
            if average > min_usage:
                min_usage = average
                most_busy = node_id

        return most_busy
