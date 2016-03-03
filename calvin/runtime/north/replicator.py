import random

from calvin.utilities.calvin_callback import CalvinCB
from calvin.utilities import calvinresponse as response
from calvin.utilities.calvinlogger import get_logger

_log = get_logger(__name__)


class Replicator(object):
    def __init__(self, node, control, required_reliability, current_nodes):
        self.node = node
        self.control = control
        self.required_reliability = required_reliability
        self.new_replicas = {}
        self.current_nodes =current_nodes

    def replicate_lost_actor(self, lost_actor_id, lost_actor_info, replica_id, replica_info, cb):
        cb = CalvinCB(self._replicate, lost_actor_id=lost_actor_id, lost_actor_info=lost_actor_info, 
                        replica_id=replica_id, replica_info=replica_info, cb=cb)
        self._delete_lost_actor(lost_actor_info, lost_actor_id, cb=cb)

    def _replicate(self, status, lost_actor_id, lost_actor_info, replica_id, replica_info, cb):
        available_nodes = []
        for node_id, link in self.node.network.links.iteritems():
            if node_id not in self.current_nodes:
                available_nodes.append(node_id)
        available_nodes = self.node.resource_manager.sort_nodes_reliability(available_nodes)

        while self.node.resource_manager.current_reliability(self.current_nodes) < self.required_reliability and len(available_nodes) > 0:
            to_node_id = available_nodes.pop(0)
            _log.info("Sending replication request of actor {} to node {}".format(replica_id, to_node_id))
            self.node.proto.actor_replication_request(replica_id, replica_info['node_id'], to_node_id, cb)
            self.current_nodes.append(to_node_id)
        print 'Current reliability: ', self.node.resource_manager.current_reliability(self.current_nodes)

    def _delete_lost_actor(self, lost_actor_info, lost_actor_id, cb):
        self._close_actor_ports(lost_actor_id, lost_actor_info)
        cb = CalvinCB(self._delete_lost_actor_cb, lost_actor_id=lost_actor_id,
                        lost_actor_info=lost_actor_info, cb=cb)
        self.node.proto.actor_destroy(lost_actor_info['node_id'], callback=cb, actor_id=lost_actor_id)
        
    def _close_actor_ports(self, lost_actor_id, lost_actor_info):
        _log.info("Closing ports of actor {}".format(lost_actor_id))
        callback = CalvinCB(self._send_disconnect_request)
        for inport in lost_actor_info['inports']:
            self.node.storage.get_port(inport['id'], callback)
        for outport in lost_actor_info['outports']:
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

    def _delete_lost_actor_cb(self, status, lost_actor_id, lost_actor_info, cb):
        _log.info("Deleting actor {} from local storage".format(lost_actor_id))
        self.node.storage.delete_actor_from_app(lost_actor_id, lost_actor_info['app_id'])
        self.node.storage.delete_actor(lost_actor_id)
        if status.status == response.SERVICE_UNAVAILABLE and not lost_actor_info['node_id'] == self.node.id:
            _log.info("Node is unavailable, delete it {}".format(lost_actor_info['node_id']))
            self.node.storage.get_node(lost_actor_info['node_id'], self._delete_node)

        if not status:
            cb(status=response.CalvinResponse(False))
        elif cb:
            cb(status=status)

    def _delete_node(self, key, value):
        _log.info("Deleting node {} with value {}".format(key, value))
        if not value:
            return

        indexed_public = value['attributes'].get('indexed_public')
        self.node.storage.delete_node(key, indexed_public)
