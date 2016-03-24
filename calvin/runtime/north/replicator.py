import random
import time
import copy

from calvin.utilities.calvin_callback import CalvinCB
from calvin.utilities import calvinresponse as response
from calvin.utilities.calvinlogger import get_logger
from calvin.utilities import calvinuuid

_log = get_logger(__name__)


class Replicator(object):
    def __init__(self, node, actor_id, actor_info, required_reliability, lost_node=None, do_delete=True):
        self.node = node
        self.actor_id = actor_id
        self.actor_info = actor_info
        self.master_node = actor_info['master_node']
        self.required_reliability = required_reliability
        self.new_replicas = {}
        self.replica_id = None
        self.replica_value = None
        self.lost_node = lost_node
        self.pending_replications = []
        self.failed_requests = []
        self.do_delete = do_delete

    def replicate_lost_actor(self, cb):
        if self.actor_info['replicate']:
            _log.info("Replicating lost actor: {}".format(self.actor_id))
            start_time_millis = int(round(time.time() * 1000))
            cb = CalvinCB(self._find_replica_nodes_cb, start_time_millis=start_time_millis, cb=cb)
            self.node.storage.get_replica_nodes(self.actor_info['app_id'], self.actor_info['name'], cb)
        else:
            _log.debug("Ignore replication of actor: {}".format(self.actor_id))
            cb(response.OK)

    ### Find a replica to replicate ###

    def _find_replica_nodes_cb(self, key, value, start_time_millis, cb):
        if not value:
            _log.error("Failed to get replica nodes for {} or no there is no replica. Storage returned {}".format(key, value))
            cb(status=response.CalvinResponse(False))
            return

        connected_nodes = self.node.network.list_links()

        _log.debug("Self {}, master {}".format(self.node.id, self.master_node))
        if not self.node.id == self.master_node:
            connected_nodes.append(self.node.id)

        _log.debug("Connected nodes: {}".format(connected_nodes))
        _log.debug("Replica nodes: {}".format(value))
        current_nodes = list(set(filter(lambda n: n in connected_nodes, value)))

        _log.debug("Current replica nodes: {}".format(current_nodes))
        if self.do_delete and self.actor_info['node_id'] in current_nodes:
            current_nodes.remove(self.actor_info['node_id'])
        elif self.lost_node in current_nodes:
            current_nodes.remove(self.lost_node)
        _log.debug("Current replica nodes: {}".format(current_nodes))

        cb = CalvinCB(self._find_app_actors, current_nodes=current_nodes, start_time_millis=start_time_millis, cb=cb)
        self.node.storage.get_application_actors(self.actor_info['app_id'], cb)

    def _find_app_actors(self, key, value, current_nodes, start_time_millis, cb):
        if not value:
            _log.error("No application-actors in storage")
            cb(status=response.NOT_FOUND)
            return
        if self.actor_id in value and self.do_delete:
            value.remove(self.actor_id)
        self._find_a_replica(value, current_nodes, start_time_millis, index=0, cb=cb)

    def _find_a_replica(self, actors, current_nodes, start_time_millis, index, cb):
        _log.debug("Searching for a replica")
        if index < len(actors) and not (self.lost_node and actors[index] == self.actor_id):
            _log.debug("Trying {}".format(actors[index]))
            cb = CalvinCB(self._check_for_original, actors=actors, current_nodes=current_nodes,
                          start_time_millis=start_time_millis, index=index, cb=cb)
            self.node.storage.get_actor(actors[index], cb=cb)
        elif index < len(actors):
            _log.debug("{} is the lost one, ignoring".format(actors[index]))
            self._find_a_replica(actors, current_nodes, start_time_millis, index + 1, cb)
        else:
            self._replicate(current_nodes, start_time_millis, cb)

    def _check_for_original(self, key, value, actors, current_nodes, start_time_millis, index, cb):
        _log.debug("Check for original: {} - {}".format(key, value))
        if value and self._is_match(value['name'], self.actor_info['name']) and value['node_id'] in self.node.network.list_links():
            _log.debug("Found a replica of lost actor: {}".format(value))
            self.replica_id = key
            self.replica_value = value
            self._replicate(current_nodes, start_time_millis, cb)
        else:
            self._find_a_replica(actors, current_nodes, start_time_millis, index + 1, cb)

    def _is_match(self, first, second):
        is_match = calvinuuid.remove_uuid(first) == calvinuuid.remove_uuid(second)
        _log.debug("{} and {} is match: {}".format(first, second, is_match))
        return is_match

    def _valid_node(self, current_nodes, node_id):
        _log.debug("Checking if {} is a valid node. Links {}. Master {}. Current {}".format(
            node_id, self.node.network.list_links(), self.master_node, current_nodes))
        if not node_id:
            return False

        if node_id not in self.node.network.list_links():
            _log.debug("{} not in network links".format(node_id))
            return False
        if node_id == self.master_node:
            _log.debug("{} is master node".format(node_id))
            return False
        if node_id in current_nodes:
            _log.debug("{} is in current nodes".format(node_id))
            return False

        return True

    def _replicate(self, current_nodes, start_time_millis, cb):
        if not self.replica_id:
            _log.error("Could not find actor to replicate")
            cb(status=response.CalvinResponse(status=response.NOT_FOUND, data=self.new_replicas))
            return

        _log.debug("Replica with current replica nodes: {}".format(current_nodes))
        current_rel, actual_rel = self.node.resource_manager.current_reliability(current_nodes, self.replica_value['type'])

        _log.debug("Current reliability: {}. Desired reliability: {}".format(current_rel, self.required_reliability))

        if current_rel > self.required_reliability:
            status = response.CalvinResponse(data=self.new_replicas)
            cb(status=status)
            return
        else:
            available_nodes = self._find_available_nodes(current_nodes)
            if not available_nodes:
                _log.error("Not enough available nodes")
                cb(status=response.CalvinResponse(status=response.NOT_FOUND, data=self.new_replicas))

            to_node_id = None
            while not self._valid_node(current_nodes, to_node_id):
                try:
                    to_node_id = available_nodes.pop(0)
                except IndexError:
                    to_node_id = None
                    break

            if not to_node_id or to_node_id not in self.node.network.list_links():
                _log.error("Not enough available nodes")
                cb(status=response.CalvinResponse(status=response.NOT_FOUND, data=self.new_replicas))
            else:
                replica_node = self.replica_value['node_id']
                self.pending_replications.append(to_node_id)
                cb = CalvinCB(func=self.collect_new_replicas, to_node_id=to_node_id, current_nodes=current_nodes,
                              start_time_millis=start_time_millis, cb=cb)
                if replica_node == self.node.id:
                    _log.debug("We have replica, replicating: {}".format(self.replica_value))
                    self.node.am.replicate(self.replica_id, to_node_id, cb)
                else:
                    _log.debug("Asking {} to replicate actor {} to node {}".format(
                        self.replica_value['node_id'], self.replica_id, to_node_id))
                    self.node.proto.actor_replication_request(self.replica_id, self.replica_value['node_id'], to_node_id, cb)

    def _find_available_nodes(self, current_nodes):
        available_nodes = []
        _log.debug("Finding available nodes among: {}".format(self.node.network.list_links()))
        connected_nodes = [self.node.id] if "gru" not in self.node.uri else []
        connected_nodes.extend(self.node.network.list_links())
        for node_id in connected_nodes:
            not_allowed = copy.deepcopy(current_nodes)
            not_allowed.extend(self.pending_replications)
            not_allowed.extend(self.failed_requests)
            not_allowed.append(self.lost_node)
            not_allowed.append(self.master_node)
            not_allowed = set(filter(None, not_allowed))
            uri = self.node.resource_manager.node_uris.get(node_id, "")
            uri = uri if uri else ""
            if node_id not in not_allowed and not self.node.is_storage_node(node_id) and not "gru" in uri:
                _log.debug("Adding {} to available nodes".format(node_id))
                available_nodes.append(node_id)

        available_nodes = self.node.resource_manager.sort_nodes_reliability(available_nodes, self.actor_info['type'])
        _log.debug("Available nodes: {}".format(available_nodes))

        return available_nodes

    def collect_new_replicas(self, status, current_nodes, to_node_id, start_time_millis, cb):
        try:
            st = str(status)
        except:
            st = ""
        _log.debug("Collect new replicas. Current: {}. Status {}. to_node_id: {}".format(current_nodes, st, to_node_id))
        self.pending_replications.remove(to_node_id)
        if status in status.success_list:
            _log.info("Successfully replicated to {}".format(to_node_id))
            self.new_replicas[status.data['actor_id']] = to_node_id
            if len(self.new_replicas) == 1:
                stop_time_millis = int(round(time.time() * 1000))
                self.node.resource_manager.update_replication_time(self.replica_value['type'], stop_time_millis - start_time_millis)
            current_nodes.append(to_node_id)
        else:
            _log.error("Failed to replicate to {}".format(to_node_id))
            self.failed_requests.append(to_node_id)

        current_nodes = list(set(filter(None, current_nodes)))
        self._replicate(current_nodes, start_time_millis, cb)
