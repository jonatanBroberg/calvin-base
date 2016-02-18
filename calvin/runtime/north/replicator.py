import re

from calvin.utilities.calvin_callback import CalvinCB
from calvin.utilities import calvinresponse as response


class Replicator(object):
    def __init__(self, node, control, actor_id_re):
        self.node = node
        self.control = control
        self.actor_id_re = actor_id_re

    def _is_match(self, first, second):
        return re.sub(self.actor_id_re, "", first) == re.sub(self.actor_id_re, "", second)

    def replicate_lost_actor(self, actors, lost_actor_id, lost_actor_info, cb):
        if lost_actor_id in actors:
            actors.remove(lost_actor_id)
        cb = CalvinCB(self._delete_lost_actor, lost_actor_info=lost_actor_info, lost_actor_id=lost_actor_id, org_cb=cb)
        self._find_and_replicate(actors, lost_actor_id, lost_actor_info, index=0, cb=cb)

    def _find_and_replicate(self, actors, lost_actor_id, lost_actor_info, index, cb):
        if index > len(actors) - 1:
            cb(status=response.CalvinResponse(False))
            return
        cb = CalvinCB(self._check_for_original, lost_actor_id=lost_actor_id, lost_actor_info=lost_actor_info,
                      actors=actors, index=index, cb=cb)
        self.node.storage.get_actor(actors[index], cb=cb)

    def _check_for_original(self, key, value, lost_actor_id, lost_actor_info, actors, index, cb):
        if not value:
            self._find_and_replicate(actors, lost_actor_id, lost_actor_info, index + 1, cb)
        elif self._is_match(value['name'], lost_actor_info['name']):
            self._replicate(key, value, lost_actor_id, lost_actor_info, cb)
        else:
            self._find_and_replicate(actors, lost_actor_id, lost_actor_info, index + 1, cb)

    def _replicate(self, actor_id, actor_info, lost_actor_id, lost_actor_info, cb):
        self.node.proto.actor_replication_request(actor_id, actor_info['node_id'], self.node.id, cb)

    def _delete_lost_actor(self, status, lost_actor_info, lost_actor_id, org_cb):
        # Failed to delete actor, may have lost node, close ports
        self._close_actor_ports(lost_actor_id, lost_actor_info, org_cb)
        if status:
            cb = CalvinCB(self._delete_lost_actor_cb, org_status=status, lost_actor_id=lost_actor_id,
                          lost_actor_info=lost_actor_info, org_cb=org_cb)
            self.node.proto.actor_destroy(lost_actor_info['node_id'], lost_actor_id, callback=cb)

    def _close_actor_ports(self, lost_actor_id, lost_actor_info, cb):
        callback = CalvinCB(self._send_disconnect_request)
        for inport in lost_actor_info['inports']:
            self.node.storage.get_port(inport['id'], cb=callback)
        for outport in lost_actor_info['outports']:
            self.node.storage.get_port(outport['id'], cb=callback)
        cb(status=response.OK)

    def _send_disconnect_request(self, key, value):
        if value:
            for (node_id, port_id) in value['peers']:
                if node_id == self.node.id:
                    self.node.pm.disconnection_request({'peer_port_id': port_id, 'port_id': key})
                else:
                    self.node.proto.port_disconnect(port_id=key, peer_node_id=node_id, peer_port_id=port_id)

    def _delete_lost_actor_cb(self, status, org_status, lost_actor_id, lost_actor_info, org_cb):
        self.node.storage.delete_actor_from_app(lost_actor_id, lost_actor_info['app_id'])
        self.node.storage.delete_actor(lost_actor_id)
        # TODO handle error
        if not status:
            org_cb(status=response.CalvinResponse(False))
        elif org_cb:
            org_cb(status=org_status)
