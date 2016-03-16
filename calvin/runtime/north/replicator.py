import random
import time

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
            _log.error("Failed to get replica nodes or no there is no replica")
            cb(status=response.CalvinResponse(False))
            return

        connected_nodes = self.node.network.list_links()
        connected_nodes.append(self.node.id)
        current_nodes = filter(lambda n: n in connected_nodes, value)

        _log.debug("Current replica nodes: {}".format(current_nodes))
        if self.do_delete and self.actor_info['node_id'] in current_nodes:
            current_nodes.remove(self.actor_info['node_id'])
        elif self.lost_node in current_nodes:
            current_nodes.remove(self.lost_node)

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
        if index < len(actors):
            cb = CalvinCB(self._check_for_original, actors=actors, current_nodes=current_nodes, 
                            start_time_millis=start_time_millis, index=index, cb=cb)
            _log.debug("Searching for actor to replicate, trying {}".format(actors[index]))
            self.node.storage.get_actor(actors[index], cb=cb)
        else:
            self._replicate(current_nodes, start_time_millis, cb)

    def _check_for_original(self, key, value, actors, current_nodes, start_time_millis, index, cb):
        _log.debug("Check for original: {} - {}".format(key, value))
        if value and self._is_match(value['name'], self.actor_info['name']) and not value['node_id'] == self.lost_node:
            _log.debug("Found an replica of lost actor:".format(key))
            self.replica_id = key
            self.replica_value = value
            self._replicate(current_nodes, start_time_millis, cb)
        else:
            self._find_a_replica(actors, current_nodes, start_time_millis, index + 1, cb)

    def _is_match(self, first, second):
        is_match = calvinuuid.remove_uuid(first) == calvinuuid.remove_uuid(second)
        _log.debug("{} and {} is match: {}".format(first, second, is_match))
        return is_match

    def _replicate(self, current_nodes, start_time_millis, cb):
        if not self.replica_id:
            _log.error("Could not find actor to replicate")
            cb(status=response.CalvinResponse(status=response.NOT_FOUND, data=self.new_replicas))
            return

        _log.info("Current replica nodes: {}".format(current_nodes))
        current_rel = self.node.resource_manager.current_reliability(current_nodes, self.replica_value['type'])

        _log.info("Current reliability: {}. Desired reliability: {}".format(current_rel, self.required_reliability))

        if current_rel > self.required_reliability:
            status = response.CalvinResponse(data=self.new_replicas)
            cb(status=status)
            return
        else:
            available_nodes = self._find_available_nodes(current_nodes)
            if not available_nodes:
                _log.error("Not enough available nodes")
                cb(status=response.CalvinResponse(status=response.NOT_FOUND, data=self.new_replicas))
            else:
                replica_node = self.replica_value['node_id']
                to_node_id = available_nodes.pop(0)
                self.pending_replications.append(to_node_id)
                cb = CalvinCB(func=self.collect_new_replicas, to_node_id=to_node_id, current_nodes=current_nodes,
                                start_time_millis=start_time_millis, cb=cb)
                if replica_node == self.node.id:
                    self.node.am.replicate(self.replica_id, to_node_id, cb)
                else:
                    _log.info("Sending replication request of actor {} to node {}".format(self.replica_id, self.replica_value['node_id']))
                    self.node.proto.actor_replication_request(self.replica_id, self.replica_value['node_id'], to_node_id, cb)

    def _find_available_nodes(self, current_nodes):
        available_nodes = []
        _log.debug("Connected to: {}".format(self.node.network.list_links()))
        connected_nodes = [self.node.id]
        connected_nodes.extend(self.node.network.list_links())
        for node_id in connected_nodes:
            if (node_id not in current_nodes and node_id not in self.pending_replications and
                    node_id not in self.failed_requests and node_id != self.lost_node and
                    not self.node.is_storage_node(node_id)):
                _log.debug("Adding {} to available nodes".format(node_id))
                available_nodes.append(node_id)

        available_nodes = self.node.resource_manager.sort_nodes_reliability(available_nodes)
        _log.info("Available nodes: {}".format(available_nodes))

        return available_nodes

    def collect_new_replicas(self, status, current_nodes, to_node_id, start_time_millis, cb):
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
        self._replicate(current_nodes, start_time_millis, cb)
