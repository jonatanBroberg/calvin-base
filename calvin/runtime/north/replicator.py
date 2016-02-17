import re

from calvin.utilities.calvin_callback import CalvinCB
from calvin.utilities import calvinresponse


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
        if status:
            cb = CalvinCB(self._delete_lost_actor_cb, org_status=status, lost_actor_id=lost_actor_id, org_cb=org_cb)
            self.node.proto.actor_destroy(lost_actor_info['node_id'], lost_actor_id, callback=cb)

    def _delete_lost_actor_cb(self, status, org_status, lost_actor_id, org_cb):
        # TODO handle error
        if not status:
            org_cb(status=calvinresponse.CalvinResponse(False))
        elif org_cb:
            org_cb(status=org_status)
