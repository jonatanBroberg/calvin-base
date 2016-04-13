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
        self.pending_replications = set()
        self.failed_requests = set()
        self.do_delete = do_delete

    @property
    def connected_nodes(self):
        connected = set(self.node.network.list_links())
        if self.node.heartbeat_actor:
            for node_id in self.node.heartbeat_actor.nodes:
                connected.add(node_id)
        connected.add(self.node.id)
        return connected

    def not_allowed(self, current_nodes):
        _log.debug("Not allowed")
        not_allowed = copy.deepcopy(current_nodes)
        _log.debug("Current: {}".format(current_nodes))
        not_allowed |= self.pending_replications
        _log.debug("Pending : {}".format(self.pending_replications))
        not_allowed |= self.failed_requests
        _log.debug("Failed: {}".format(self.failed_requests))
        not_allowed.add(self.lost_node)
        _log.debug("Lost node: {}".format(self.lost_node))
        not_allowed.add(self.master_node)
        _log.debug("Master node: {}".format(self.master_node))
        not_allowed = set(filter(None, not_allowed))

        _log.debug("Not allowed: {}".format(not_allowed))
        return not_allowed

    def replicate_lost_actor(self, cb, start_time_millis):
        for node_id in self.connected_nodes:
            self.node.network.link_request(node_id, timeout=0.1)
        if self.actor_info['replicate']:
            _log.info("Replicating lost actor: {}".format(self.actor_info))
            #time.sleep(1)
            cb = CalvinCB(self._find_replica_nodes_cb, start_time_millis=start_time_millis, cb=cb)
            self.node.storage.get_replica_nodes(self.actor_info['app_id'], self.actor_info['name'], cb)
        else:
            _log.debug("Ignore replication of actor: {}".format(self.actor_id))
            cb(status=response.CalvinResponse())

    ### Find a replica to replicate ###

    def _find_replica_nodes_cb(self, key, value, start_time_millis, cb, prev_current_nodes=set()):
        if not value:
            _log.error("Failed to get replica nodes for {} or no there is no replica. Storage returned {}".format(key, value))
            cb(status=response.CalvinResponse(False))
            return

        connected_nodes = set(self.node.network.list_links())
        _log.debug("Connected nodes: {}".format(connected_nodes))
        _log.info("Storage returned replica nodes: {}".format(value))

        current_nodes = set(value)

        if self.do_delete and self.actor_info['node_id'] in current_nodes:
            _log.debug("Removing node {} from current nodes".format(self.actor_info['node_id']))
            current_nodes.remove(self.actor_info['node_id'])
        elif self.lost_node in current_nodes:
            _log.debug("Removing lost node {} from current nodes".format(self.lost_node))
            current_nodes.remove(self.lost_node)

        _log.debug("Joining with previous current nodes: {}".format(prev_current_nodes))
        current_nodes = set(current_nodes) | set(prev_current_nodes)

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

        random.shuffle(value)

        self._find_a_replica(value, current_nodes, start_time_millis, index=0, cb=cb)

    def _find_a_replica(self, actors, current_nodes, start_time_millis, index, cb):
        _log.debug("Searching for a replica for {}".format(self.actor_info))
        if index < len(actors):
            if self.lost_node and actors[index] == self.actor_id:
                _log.debug("{} is the lost one, ignoring".format(actors[index]))
                return self._find_a_replica(actors, current_nodes, start_time_millis, index + 1, cb)

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
        links = set(self.node.network.list_links())
        if not value:
            return self._find_a_replica(actors, current_nodes, start_time_millis, index + 1, cb)
        elif value['node_id'] not in links:
            _log.warning("Not connected to node {} of actor {}".format(value['node_id'], value))
            return self._find_a_replica(actors, current_nodes, start_time_millis, index + 1, cb)
        elif self._is_match(value['name'], self.actor_info['name']):
            _log.debug("Found a replica of lost actor: {}".format(value))
            self.replica_id = key
            self.replica_value = value
            return self._replicate(current_nodes, start_time_millis, cb)

        return self._find_a_replica(actors, current_nodes, start_time_millis, index + 1, cb)

    def _is_match(self, first, second):
        is_match = calvinuuid.remove_uuid(first) == calvinuuid.remove_uuid(second)
        _log.debug("{} and {} is match: {}".format(first, second, is_match))
        return is_match

    def _valid_node(self, current_nodes, node_id):
        links = set(self.node.network.list_links())
        _log.debug("Checking if {} is a valid node. Links {}. Master {}. Current {}".format(
            node_id, links, self.master_node, current_nodes))
        if not node_id:
            return False

        if node_id != self.node.id and node_id not in links:
            _log.debug("{} not in network links".format(node_id))
            return False
        if node_id == self.master_node:
            _log.debug("{} is master node".format(node_id))
            return False
        if node_id in current_nodes:
            _log.debug("{} is in current nodes".format(node_id))
            return False

        _log.debug("{} is a valid node".format(node_id))
        return True

    def _replicate(self, current_nodes, start_time_millis, cb):
        if not self.replica_id:
            _log.error("Could not find actor to replicate")
            cb(status=response.CalvinResponse(status=response.NOT_FOUND, data=self.new_replicas))
            return

        _log.debug("Replica with current replica nodes: {}".format(current_nodes))
        actual_rel = self.node.resource_manager.current_reliability(current_nodes, self.replica_value['type'])

        _log.debug("Current reliability: {}. Desired reliability: {}".format(actual_rel, self.required_reliability))

        if actual_rel > self.required_reliability:
            status = response.CalvinResponse(data=self.new_replicas)
            cb(status=status)
            return
        else:
            available_nodes = self._find_available_nodes(current_nodes)
            if not available_nodes:
                _log.error("Not enough available nodes")
                cb(status=response.CalvinResponse(status=response.NOT_FOUND, data=self.new_replicas))
                return

            to_node_id = None
            while not self._valid_node(current_nodes, to_node_id):
                try:
                    to_node_id = available_nodes.pop(0)
                except IndexError:
                    to_node_id = None
                    break

            connected = set(self.node.network.list_links())
            if self.node.id != self.master_node:
                connected.add(self.node.id)

            if not to_node_id or to_node_id not in connected:
                _log.error("Not enough available nodes")
                cb(status=response.CalvinResponse(status=response.NOT_FOUND, data=self.new_replicas))
                return
            else:
                replica_node = self.replica_value['node_id']
                self.pending_replications.add(to_node_id)
                cb = CalvinCB(func=self.collect_new_replicas, to_node_id=to_node_id, current_nodes=current_nodes,
                              start_time_millis=start_time_millis, cb=cb)
                if replica_node == self.node.id:
                    _log.info("We have replica, replicating: {}".format(self.replica_value))
                    self.node.am.replicate(self.replica_id, to_node_id, cb)
                else:
                    _log.info("Asking {} to replicate actor {} to node {}".format(
                        self.replica_value['node_id'], self.replica_id, to_node_id))
                    _log.info("Asking {} - {}".format(to_node_id, self.node.resource_manager.node_uris.get(to_node_id)))
                    self.node.proto.actor_replication_request(self.replica_id, self.replica_value['node_id'], to_node_id, cb)

    def _find_available_nodes(self, current_nodes):
        available_nodes = set()
        connected_nodes = self.connected_nodes
        _log.debug("Finding available nodes among: {}".format(connected_nodes))

        not_allowed = self.not_allowed(current_nodes)
        links = set(self.node.network.list_links())
        for node_id in connected_nodes:
            uri = self.node.resource_manager.node_uris.get(node_id, "")
            uri = uri if uri else ""
            if node_id not in links:
                _log.debug("Node {} not in links".format(node_id))
                continue
            try:
                if node_id not in not_allowed and not self.node.is_storage_node(node_id) and not "gru" in uri:
                    _log.debug("Adding {} to available nodes".format(node_id))
                    available_nodes.add(node_id)
            except Exception as e:
                _log.warning(e)
                pass

        available_nodes = self.node.resource_manager.sort_nodes_reliability(available_nodes, self.actor_info['type'])
        _log.debug("Available nodes: {}".format(available_nodes))

        return available_nodes

    def collect_new_replicas(self, status, current_nodes, to_node_id, start_time_millis, cb):
        if status and status.data and 'actor_id' in status.data:
            actor_id = status.data['actor_id']
            _log.debug("Node {} returned actor id {}".format(to_node_id, actor_id))
            self.node.storage.add_node_actor(to_node_id, actor_id)

        _log.debug("Collect new replicas. Current: {}. Status {}. to_node_id: {}".format(current_nodes, status, to_node_id))
        if to_node_id in self.pending_replications:
            self.pending_replications.remove(to_node_id)
        if status in status.success_list:
            _log.info("Successfully replicated to {}".format(to_node_id))
            self.new_replicas[status.data['actor_id']] = to_node_id
            if len(self.new_replicas) == 1:
                stop_time_millis = int(round(time.time() * 1000))
                self.node.report_replication_time(self.replica_value['type'], stop_time_millis - start_time_millis)
            current_nodes.add(to_node_id)
        else:
            _log.error("Failed to replicate to {} - {}".format(to_node_id, status))
            self.failed_requests.add(to_node_id)

        cb = CalvinCB(self._find_replica_nodes_cb, start_time_millis=start_time_millis, prev_current_nodes=current_nodes, cb=cb)
        self.node.storage.get_replica_nodes(self.actor_info['app_id'], self.actor_info['name'], cb)
