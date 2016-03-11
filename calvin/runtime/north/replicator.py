import random
import re

from calvin.utilities.calvin_callback import CalvinCB
from calvin.utilities import calvinresponse as response
from calvin.utilities.calvinlogger import get_logger

_log = get_logger(__name__)


class Replicator(object):
    def __init__(self, node, lost_actor_id, lost_actor_info, required_reliability, uuid_re, lost_node=None):
        self.node = node
        self.lost_actor_id = lost_actor_id
        self.lost_actor_info = lost_actor_info
        self.required_reliability = required_reliability
        self.new_replicas = {}
        self.uuid_re = uuid_re
        self.replica_id = None
        self.replica_value = None
        self.lost_node = lost_node
        self.pending_replications = []
        self.failed_requests = []

    def replicate_lost_actor(self, cb):
        _log.info("Replicating lost actor: {}".format(self.lost_actor_id))
        cb = CalvinCB(self._find_replica_nodes, cb=cb)
        self._delete_lost_actor(cb=cb)

    def _find_replica_nodes(self, cb):
        cb = CalvinCB(self._find_replica_nodes_cb, cb=cb)
        self.node.storage.get_replica_nodes(self.lost_actor_info['app_id'], self.lost_actor_info['name'], cb)

    ### Find a replica to replicate ###

    def _find_replica_nodes_cb(self, key, value, cb):
        if not value:
            _log.error("Failed to get replica nodes or no there is no replica")
            cb(response.CalvinResponse(False))
            return

        connected_nodes = self.node.network.list_links()
        current_nodes = filter(lambda n: n in connected_nodes, value)

        _log.info("Current replica nodes: {}".format(current_nodes))
        if self.lost_actor_info['node_id'] in current_nodes:
            _log.info("Removing lost node from current nodes")
            current_nodes.remove(self.lost_actor_info['node_id'])
        elif self.lost_node in current_nodes:
            _log.info("Removing lost node from current nodes")
            current_nodes.remove(self.lost_node)

        cb = CalvinCB(self._find_app_actors, current_nodes=current_nodes, cb=cb)
        self.node.storage.get_application_actors(self.lost_actor_info['app_id'], cb)

    def _find_app_actors(self, key, value, current_nodes, cb):
        if not value:
            _log.error("No application-actors in storage")
            cb(response.NOT_FOUND)
            return
        if self.lost_actor_id in value:
            value.remove(self.lost_actor_id)
        self._find_a_replica(value, current_nodes, index=0, cb=cb)

    def _find_a_replica(self, actors, current_nodes, index, cb):
        if index < len(actors):
            cb = CalvinCB(self._check_for_original, actors=actors, current_nodes=current_nodes, index=index, cb=cb)
            _log.debug("Searching for actor to replicate, trying {}".format(actors[index]))
            self.node.storage.get_actor(actors[index], cb=cb)
        else:
            self._replicate(current_nodes, cb)

    def _check_for_original(self, key, value, actors, current_nodes, index, cb):
        _log.info("Check for original: {} - {}".format(key, value))
        if value and self._is_match(value['name'], self.lost_actor_info['name']):
            _log.debug("Found an replica of lost actor:".format(key))
            self.replica_id = key
            self.replica_value = value
            self._replicate(current_nodes, cb)
        else:
            self._find_a_replica(actors, current_nodes, index + 1, cb)

    def _is_match(self, first, second):
        is_match = re.sub(self.uuid_re, "", first) == re.sub(self.uuid_re, "", second)
        _log.debug("{} and {} is match: {}".format(first, second, is_match))
        return is_match

    def _replicate(self, current_nodes, cb):
        if self.replica_id is not None:
            _log.info("current nodes: {}".format(current_nodes))
            _log.info("connected to: {}".format(self.node.network.list_links()))
            current_rel = self.node.resource_manager.current_reliability(current_nodes)
            _log.info("Current reliability: {}. Desired reliability: {}".format(current_rel, self.required_reliability))
            if current_rel > self.required_reliability:
                status = response.CalvinResponse(data=self.new_replicas)
                cb(status=status)
                return
            else:
                available_nodes = []
                for (node_id, link) in self.node.network.links.iteritems():
                    _log.debug("Connected to node {}".format(node_id))
                    if (node_id not in current_nodes and node_id not in self.pending_replications and
                            node_id not in self.failed_requests and node_id != self.lost_node and
                            not self.node.is_storage_node(node_id)):
                        _log.debug("Adding to available nodes")
                        available_nodes.append(node_id)
                _log.debug("Available nodes: {}".format(available_nodes))
                available_nodes = self.node.resource_manager.sort_nodes_reliability(available_nodes)
                _log.debug("Available nodes after sorting: {}".format(available_nodes))
                if len(available_nodes) == 0:
                    cb(response.CalvinResponse(status=response.NOT_FOUND, data=self.new_replicas))
                    _log.error("Not enough available nodes")
                else:
                    replica_node = self.replica_value['node_id']
                    to_node_id = available_nodes.pop(0)
                    self.pending_replications.append(to_node_id)
                    cb = CalvinCB(func=self.collect_new_replicas, to_node_id=to_node_id, current_nodes=current_nodes,
                                  cb=cb)
                    if replica_node == self.node.id:
                        self.node.am.replicate(self.replica_id, to_node_id, cb)
                    else:
                        _log.info("Sending replication request of actor {} to node {}".format(self.replica_id, self.replica_value['node_id']))
                        self.node.proto.actor_replication_request(self.replica_id, self.replica_value['node_id'], to_node_id, cb)
        else:
            cb(response.CalvinResponse(status=response.NOT_FOUND, data=self.new_replicas))
            _log.warning("Could not find actor to replicate")

    def collect_new_replicas(self, status, current_nodes, to_node_id, cb):
        self.pending_replications.remove(to_node_id)
        if status in status.success_list:
            _log.info("Successfully replicated to {}".format(to_node_id))
            self.new_replicas[status.data['actor_id']] = to_node_id
            current_nodes.append(to_node_id)
        else:
            _log.error("Failed to replicate to {}".format(to_node_id))
            self.failed_requests.append(to_node_id)
        self._replicate(current_nodes, cb)

    ### Delete information about lost actor ###

    def _delete_lost_actor(self, cb):
        self._close_actor_ports()
        cb = CalvinCB(self._delete_lost_actor_cb, cb=cb)
        actor_node = self.lost_actor_info['node_id']
        if self.lost_node == actor_node:
            cb(status=response.CalvinResponse(response.SERVICE_UNAVAILABLE))
        else:
            self.node.proto.actor_destroy(actor_node, callback=cb, actor_id=self.lost_actor_id)

    def _close_actor_ports(self):
        _log.info("Closing ports of actor {}".format(self.lost_actor_id))
        callback = CalvinCB(self._send_disconnect_request)
        for inport in self.lost_actor_info['inports']:
            self.node.storage.get_port(inport['id'], callback)
        for outport in self.lost_actor_info['outports']:
            self.node.storage.get_port(outport['id'], callback)

    def _send_disconnect_request(self, key, value):
        if not value:
            return

        _log.debug("Sending disconnect request for port {}".format(key))
        for (node_id, port_id) in value['peers']:
            if self.lost_node and node_id == self.lost_node:
                _log.debug("No need to send to lost node")
                continue
            if node_id == 'local' or node_id == self.node.id:
                _log.debug("Node {} is local. Asking port manager to close peer_port_id {}, port_id {}".format(
                    node_id, port_id, key))
                self.node.pm.disconnection_request({'peer_port_id': port_id, 'port_id': key})
            else:
                _log.debug("Node {} is remote. Sending port disconnect request for port_id {}, \
                           peer_node_id {} and peer_port_id {}".format(node_id, key, node_id, port_id))
                self.node.proto.port_disconnect(port_id=key, peer_node_id=node_id, peer_port_id=port_id)

    def _delete_lost_actor_cb(self, status, cb):
        _log.info("Deleting actor {} from local storage".format(self.lost_actor_id))
        self.node.storage.delete_actor_from_app(self.lost_actor_info['app_id'], self.lost_actor_id)
        self.node.storage.delete_actor(self.lost_actor_id)
        #if status.status == response.SERVICE_UNAVAILABLE and not self.lost_actor_info['node_id'] == self.node.id:
        #    _log.info("Node is unavailable, delete it {}".format(self.lost_actor_info['node_id']))
        #    self.node.storage.get_node(self.lost_actor_info['node_id'], self._delete_node)
        cb()

    #def _delete_node(self, key, value):
    #    _log.info("Deleting node {} with value {}".format(key, value))
    #    if not value:
    #        return

    #    indexed_public = value['attributes'].get('indexed_public')
    #    self.node.storage.delete_node(key, indexed_public)
