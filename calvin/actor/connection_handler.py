import calvin.utilities.calvinresponse as response
from calvin.utilities.calvin_callback import CalvinCB
from calvin.utilities.calvinlogger import get_logger

_log = get_logger(__name__)


class ConnectionHandler(object):
    def __init__(self, node):
        self.node = node
        self._pending_connections = {}

    def setup_connections(self, actor, prev_connections=None, connection_list=None, callback=None):
        _log.debug("Setting up connections for actor {}, prev_connections {}, connection_list {}".format(
            actor, prev_connections, connection_list))
        if prev_connections:
            # Convert prev_connections to connection_list format
            connection_list = self._prev_connections_to_connection_list(prev_connections)

        if connection_list:
            # Migrated actor
            self.connect(actor, connection_list, callback=callback)

    def setup_replica_connections(self, actor, state, prev_connections, callback=None):
        _log.debug("Setting up replica connections for actor {}, prev_connections {}".format(
            actor, prev_connections))
        connection_list = self._prev_connections_to_connection_list(prev_connections)

        port_id_translations = self._translate_port_ids(actor, prev_connections)
        connection_list = self._translate_connection_list(actor, connection_list, port_id_translations)
        state = self._translate_state(actor, state, port_id_translations)

        callback = CalvinCB(self._set_port_states, actor=actor, state=state, callback=callback)
        if not connection_list and callback:
            callback(status=response.CalvinResponse(False))
        else:
            self._pending_connections[actor.id] = []
            callback = CalvinCB(self._setup_replica_connections, actor_id=actor.id, expected=len(connection_list), callback=callback)
            self.connect(actor, connection_list, state, callback=callback)

    def _setup_replica_connections(self, status, actor_id, expected, callback, *args, **kwargs):
        self._pending_connections[actor_id].append(status)
        if len(self._pending_connections[actor_id]) == expected:
            callback(status=response.CalvinResponse(status=all(self._pending_connections[actor_id])))
            del self._pending_connections[actor_id]

    def connections(self, actor):
        return actor.connections(self.node.id)

    def _set_port_states(self, actor, state, callback, *args, **kwargs):
        for port_id in state['inports']:
            port_name = state['inports'][port_id]['name']
            actor.inports[port_name]._set_state(state['inports'][port_id])
        for port_id in state['outports']:
            port_name = state['outports'][port_id]['name']
            actor.outports[port_name]._set_state(state['outports'][port_id])

        if callback:
            callback(*args, **kwargs)

    def connect(self, actor, connection_list, port_states=None, callback=None):
        """
        Reconnecting the ports can be done using a connection_list
        of tuples (node_id i.e. our id, port_id, peer_node_id, peer_port_id)
        """
        _log.info("Connecting actor {}, connection_list {}".format(actor.name, connection_list))
        peer_port_ids = [c[3] for c in connection_list]

        for node_id, port_id, peer_node_id, peer_port_id in connection_list:
            self.node.pm.connect(actor_id=actor.id,
                                 port_id=port_id,
                                 peer_node_id=peer_node_id,
                                 peer_port_id=peer_port_id,
                                 port_states=port_states,
                                 callback=CalvinCB(self._actor_connected,
                                                   peer_port_id=peer_port_id,
                                                   actor_id=actor.id,
                                                   peer_port_ids=peer_port_ids,
                                                   _callback=callback))

    def _actor_connected(self, status, peer_port_id, actor_id, peer_port_ids, _callback, **kwargs):
        """ Get called for each of the actor's ports when connecting, but callback should only be called once
            status: success or not
            _callback: original callback
            peer_port_ids: list of port ids kept in context between calls when *changed* by this function,
                           do not replace it
        """
        # Send negative response if not already done it
        if not status and peer_port_ids:
            if _callback:
                del peer_port_ids[:]
                _callback(status=response.CalvinResponse(False), actor_id=actor_id)
        if peer_port_id in peer_port_ids:
            # Remove this port from list
            peer_port_ids.remove(peer_port_id)
            # If all ports done send OK
            if not peer_port_ids:
                if _callback:
                    _callback(status=response.CalvinResponse(True), actor_id=actor_id)

    def _prev_connections_to_connection_list(self, prev_connections):
        """Convert prev_connection format to connection_list format"""
        cl = []
        for conn in prev_connections['inports']:
            cl.append((conn['node_id'], conn['port_id'], conn['peer_node_id'], conn['peer_port_id']))
        for conn in prev_connections['outports']:
            cl.append((conn['node_id'], conn['port_id'], conn['peer_node_id'], conn['peer_port_id']))
        return cl

    def _translate_port_ids(self, actor, prev_connections):
        port_id_translations = {}
        port_names = prev_connections['port_names']
        for (port_id, port_name) in port_names['inports'].iteritems():
            port_id_translations[port_id] = actor.inports[port_name].id
        for (port_id, port_name) in port_names['outports'].iteritems():
            port_id_translations[port_id] = actor.outports[port_name].id

        return port_id_translations

    def _translate_connection_list(self, actor, connection_list, port_id_translations):
        """After replicating an actor, the list of previous connections
        contains port_ids for the original actor and must be updated.

        Args:
            connection_list: [(_, port_id, _, _), ...]
        Returns:
            [(_, updated_port_id, _, _), ...]
        """
        if not connection_list:
            return []

        translated_connection_list = []
        if port_id_translations:
            for node_id, port_id, peer_node_id, peer_port_id in connection_list:
                translated_connection_list.append((self.node.id, port_id_translations[port_id], peer_node_id, peer_port_id))

        return translated_connection_list

    def _translate_state(self, actor, state, port_id_translations):
        """Translates the port IDs in state inports and outports to match IDs for the new
        replica.
        """
        inports = state['inports']
        outports = state['outports']

        new_inports = self._translate_inports(port_id_translations, inports)
        new_outports = self._translate_outports(port_id_translations, outports)
        state['inports'] = new_inports
        state['outports'] = new_outports

        return state

    def _translate_inports(self, port_id_translations, ports):
        new_ports = {}
        for port_name in ports:
            port = ports[port_name]
            if not port:
                continue

            fifo = port['fifo']

            new_readers = []
            for reader in fifo['readers']:
                reader_parts = reader.split("_")
                port_id = reader_parts[0]
                if port_id in port_id_translations:
                    new_readers.append("_".join([port_id_translations[port_id], reader_parts[1]]))

            new_tentative_read_pos = {}
            for reader in fifo['tentative_read_pos']:
                reader_parts = reader.split("_")
                port_id = reader_parts[0]
                val = fifo['tentative_read_pos'][reader]
                if port_id in port_id_translations:
                    new_key = "_".join([port_id_translations[port_id], reader_parts[1]])
                    new_tentative_read_pos[new_key] = val

            new_read_pos = {}
            for reader in fifo['read_pos']:
                reader_parts = reader.split("_")
                port_id = reader_parts[0]
                val = fifo['read_pos'][reader]
                if port_id in port_id_translations:
                    new_key = "_".join([port_id_translations[port_id], reader_parts[1]])
                    new_read_pos[new_key] = val

            new_fifo = {}
            for reader in fifo['fifo']:
                reader_parts = reader.split("_")
                port_id = reader_parts[0]
                val = fifo['fifo'][reader]
                if port_id in port_id_translations:
                    new_key = "_".join([port_id_translations[port_id], reader_parts[1]])
                    new_fifo[new_key] = val

            new_write_pos = {}
            for reader in fifo['write_pos']:
                reader_parts = reader.split("_")
                port_id = reader_parts[0]
                val = fifo['write_pos'][reader]
                if port_id in port_id_translations:
                    new_key = "_".join([port_id_translations[port_id], reader_parts[1]])
                    new_write_pos[new_key] = val

            catchup_fifo_key = fifo['readers'][0] if fifo['readers'] else None
            new_ports[port_id_translations[port['id']]] = {
                'name': port['name'],
                'fifo': {
                    'readers': new_readers,
                    'write_pos': new_write_pos,
                    'N': fifo['N'],
                    'tentative_read_pos': new_tentative_read_pos,
                    'read_pos': new_read_pos,
                    'fifo': new_fifo,
                    'catchup_fifo_key': catchup_fifo_key
                },
                'id': port_id_translations[port['id']]
            }

        return new_ports

    def _translate_outports(self, port_id_translations, ports):
        new_ports = {}
        for port_name in ports:
            port = ports[port_name]
            if not port:
                continue

            fifo = port['fifo']

            new_readers = []
            for reader in fifo['readers']:
                reader_parts = reader.split("_")
                port_id = reader_parts[1]
                if port_id in port_id_translations:
                    new_readers.append("_".join([reader_parts[0], port_id_translations[port_id]]))

            new_tentative_read_pos = {}
            for reader in fifo['tentative_read_pos']:
                reader_parts = reader.split("_")
                port_id = reader_parts[1]
                if port_id in port_id_translations:
                    new_key = "_".join([reader_parts[0], port_id_translations[port_id]])
                    new_tentative_read_pos[new_key] = fifo['tentative_read_pos'][reader]

            new_read_pos = {}
            for reader in fifo['read_pos']:
                reader_parts = reader.split("_")
                port_id = reader_parts[1]
                if port_id in port_id_translations:
                    new_key = "_".join([reader_parts[0], port_id_translations[port_id]])
                    new_read_pos[new_key] = fifo['read_pos'][reader]

            new_write_pos = {}
            for reader in fifo['write_pos']:
                reader_parts = reader.split("_")
                port_id = reader_parts[1]
                val = fifo['write_pos'][reader]
                if port_id in port_id_translations:
                    new_key = "_".join([reader_parts[0], port_id_translations[port_id]])
                    new_write_pos[new_key] = val

            new_fifo = {}
            for reader in fifo['fifo']:
                reader_parts = reader.split("_")
                port_id = reader_parts[1]
                val = fifo['fifo'][reader]
                if port_id in port_id_translations:
                    new_key = "_".join([reader_parts[0], port_id_translations[port_id]])
                    new_fifo[new_key] = val

            new_ports[port_id_translations[port['id']]] = {
                'name': port['name'],
                'fanout': port['fanout'],
                'fifo': {
                    'readers': new_readers,
                    'write_pos': new_write_pos,
                    'N': fifo['N'],
                    'tentative_read_pos': new_tentative_read_pos,
                    'read_pos': new_read_pos,
                    'fifo': new_fifo
                },
                'id': port_id_translations[port['id']]
            }

        return new_ports
