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
