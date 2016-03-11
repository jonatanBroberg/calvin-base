
from calvin.utilities.calvin_callback import CalvinCB
from calvin.utilities.calvinlogger import get_logger
import calvin.utilities.calvinresponse as response
from calvin.runtime.north.replicator import Replicator

_log = get_logger(__name__)


class LostNodeHandler(object):

    def __init__(self, node, resource_manager, port_manager, actor_manager, storage, uuid_re):
        self.node = node
        self._lost_nodes = []
        self.resource_manager = resource_manager
        self.pm = port_manager
        self.am = actor_manager
        self.storage = storage
        self.uuid_re = uuid_re

    def handle_lost_node(self, node_id):
        if node_id in self._lost_nodes:
            _log.info("Got multiple lost node signals")
            return

        try:
            self.resource_manager.lost_node(node_id, self.node.peer_uris[node_id])
        except:
            _log.info(self.node.id)
            _log.info(node_id)
            _log.info(self.node.peer_uris)

        _log.info("Lost node {}".format(node_id))
        highest_prio_node = self._highest_prio_node(node_id)
        if highest_prio_node == self.node.id:
            self._lost_nodes.append(node_id)
            #_log.info("We have highest id, replicate actors")
            self.replicate_node_actors(node_id, cb=CalvinCB(self._lost_node_cb, node_id=node_id))

        self.pm.close_all_ports_to_node(self.am.actors.values(), node_id)

    def _delete_node(self, key, value):
        _log.info("Deleting node {} with value {}".format(key, value))
        if not value:
            return

        indexed_public = value['attributes'].get('indexed_public')
        self.storage.delete_node(key, indexed_public)

    def _lost_node_cb(self, status, node_id):
        if not status:
            _log.error("Failed to handle lost node: {}".format(status))
        else:
            _log.info("Successfully handled lost node")
        self.storage.get_node(node_id, self._delete_node)
        if node_id in self._lost_nodes:
            self._lost_nodes.remove(node_id)

    def _highest_prio_node(self, node_id):
        _log.debug("Getting highest_prio_node")
        node_ids = self.node.network.list_links()
        if not node_ids:
            _log.info("We are not connected to anyone")
            # We are not connected to anyone
            return None

        if node_id in node_ids:
            node_ids.remove(node_id)

        if self.node.id not in node_ids:
            node_ids.append(self.node.id)

        node_ids = [n_id for n_id in node_ids if not self.node.is_storage_node(n_id)]
        if not node_ids:
            return None

        _log.debug("highest prio node: {}".format(sorted(node_ids)[0]))
        return sorted(node_ids)[0]

    def replicate_node_actors(self, node_id, cb):
        _log.info("Fetching actors for lost node: {}".format(node_id))
        try:
            self.storage.get_node_actors(node_id, cb=CalvinCB(self._replicate_node_actors, node_id=node_id, cb=cb))
        except AttributeError as e:
            _log.warning("Failed to get node actors: {}".format(e))
            # We are the deleted node
            pass

    def _replicate_node_actors(self, key, value, node_id, cb):
        _log.info("Replicating lost actors {}".format(value))
        if value is None:
            _log.warning("Storage returned None when fetching node actors for node: {} - {}".format(
                node_id, self.resource_manager.node_uris[node_id]))
            cb(status=response.CalvinResponse(False))
            return
        elif value == []:
            _log.debug("No value returned from storage when fetching node actors")
            cb(status=response.CalvinResponse(True))
            return

        for actor_id in value:
            self.storage.get_actor(actor_id, cb=CalvinCB(self._replicate_node_actor, lost_node_id=node_id,
                                   lost_actor_id=actor_id, cb=cb))

    def _replicate_node_actor(self, key, value, lost_node_id, lost_actor_id, cb):
        """ Get app id and actor name from actor info """
        _log.info("Replicating node actor {}: {}".format(key, value))
        if not value:
            _log.error("Failed get lost actor info from storage")
            cb(response.CalvinResponse(False))
            return

        cb = CalvinCB(func=self._handle_lost_application_actor, lost_node_id=lost_node_id,
                      lost_actor_id=lost_actor_id, lost_actor_info=value, cb=cb)
        self.storage.get_application(value['app_id'], cb=cb)

    def _handle_lost_application_actor(self, key, value, lost_node_id, lost_actor_id, lost_actor_info, cb):
        """ Get required reliability from app info """
        if not value:
            _log.error("Failed to get application actors")
            return

        replicator = Replicator(self.node, lost_actor_id, lost_actor_info, value['required_reliability'],
                                self.uuid_re, lost_node=lost_node_id)
        cb = CalvinCB(self._delete_actor, actor_id=lost_actor_id, app_id=lost_actor_info['app_id'], cb=cb,
                      lost_node_id=lost_node_id)
        replicator.replicate_lost_actor(cb)

    def _delete_actor(self, status, lost_node_id, actor_id, app_id, cb):
        _log.info("Deleting actor {} from local storage".format(actor_id))
        self.storage.delete_actor_from_app(app_id, actor_id)
        self.storage.delete_actor(actor_id)
        cb(status=status, node_id=lost_node_id)
