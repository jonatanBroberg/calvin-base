import random
import re
import time

from calvin.utilities.calvin_callback import CalvinCB
from calvin.utilities import calvinresponse as response
from calvin.utilities.calvinlogger import get_logger

_log = get_logger(__name__)


class Replicator(object):
    def __init__(self, node, lost_actor_id, lost_actor_info, required_reliability, uuid_re):
        self.node = node
        self.lost_actor_id = lost_actor_id
        self.lost_actor_info = lost_actor_info
        self.required_reliability = required_reliability
        self.new_replicas = {}
        self.uuid_re = uuid_re

    def replicate_lost_actor(self, cb):
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
        
        current_nodes=value
        if self.lost_actor_info['node_id'] in current_nodes:
            current_nodes.remove(self.lost_actor_info['node_id'])
        cb = CalvinCB(self._find_app_actors, current_nodes=value, cb=cb)
        self.node.storage.get_application_actors(self.lost_actor_info['app_id'], cb)

    def _find_app_actors(self, key, value, current_nodes, cb):
        """ Search for a replica"""
        if not value:
            cb(response.CalvinResponse(False))
            return

        if self.lost_actor_id in value:
            value.remove(self.lost_actor_id)
        self._find_replicas(value, current_nodes=current_nodes, index=0, cb=cb)

    def _find_replicas(self, actors, current_nodes, index, cb):
        cb = CalvinCB(self._check_for_original, actors=actors, current_nodes=current_nodes, index=index, cb=cb)
        _log.debug("Searching for actor to replicate, trying {}".format(actors[index]))
        self.node.storage.get_actor(actors[index], cb=cb)

    def _check_for_original(self, key, value, actors, current_nodes, index, cb):
        if value and self._is_match(value['name'], self.lost_actor_info['name']):
            _log.info("Found an replica of lost actor: {}".format(key))
            self._replicate(key, value['node_id'], current_nodes, cb)
            return

        if index < len(actors) - 1:
            self._find_replicas(actors, current_nodes, index + 1, cb)
        else:
            cb(response.CalvinResponse(False))

    def _is_match(self, first, second):
        is_match = re.sub(self.uuid_re, "", first) == re.sub(self.uuid_re, "", second)
        _log.debug("{} and {} is match: ".format(first, second, is_match))
        return is_match

    ### Replicate the replica ###

    def _replicate(self, replica_id, replica_node, current_nodes, cb):
        _log.info("Replicating lost actor: {}".format(self.lost_actor_id))
        time.sleep(0.5)
        
        available_nodes = []
        for node_id, link in self.node.network.links.iteritems():
            if node_id not in current_nodes:
                available_nodes.append(node_id)
        available_nodes = self.node.resource_manager.sort_nodes_reliability(available_nodes)

        while self.node.resource_manager.current_reliability(current_nodes) < self.required_reliability and len(available_nodes) > 0:
            to_node_id = available_nodes.pop(0)
            _log.info("Sending replication request of actor {} to node {}".format(replica_id, to_node_id))
            self.node.proto.actor_replication_request(replica_id, replica_node, to_node_id, self._collect_new_replicas)
            current_nodes.append(to_node_id)
        cb(response.OK)

    def _collect_new_replicas(self, status):
        self.new_replicas.append(status.data)

    ### Delete information of old lost actor ###

    def _delete_lost_actor(self, cb):
        self._close_actor_ports()
        # Check if lost_actor_info['node_id'] == self.node then call cb directly?
        self.node.proto.actor_destroy(self.lost_actor_info['node_id'], callback=CalvinCB(self._delete_lost_actor_cb, cb=cb), actor_id=self.lost_actor_id)

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
        #self.node.storage.delete_actor_from_app(self.lost_actor_info['app_id'], self.lost_actor_id)
        #self.node.storage.delete_actor(self.lost_actor_id)
        if status.status == response.SERVICE_UNAVAILABLE and not self.lost_actor_info['node_id'] == self.node.id:
            _log.info("Node is unavailable, delete it {}".format(self.lost_actor_info['node_id']))
            self.node.storage.get_node(self.lost_actor_info['node_id'], self._delete_node)

        if not status:
            _log.error('Failed to delete lost actor info')
        cb()

    def _delete_node(self, key, value):
        _log.info("Deleting node {} with value {}".format(key, value))
        if not value:
            return

        indexed_public = value['attributes'].get('indexed_public')
        self.node.storage.delete_node(key, indexed_public)
