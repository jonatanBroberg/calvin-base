# -*- coding: utf-8 -*-

# Copyright (c) 2015 Ericsson AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import traceback
import sys
import json
import time
from datetime import datetime
from calvin.utilities import calvinuuid
from calvin.utilities.utils import enum
from calvin.utilities.calvin_callback import CalvinCB, CalvinCBClass
import calvin.utilities.calvinresponse as response

from calvin.utilities import calvinlogger
_log = calvinlogger.get_logger(__name__)

class CalvinTunnel(object):
    """CalvinTunnel is a tunnel over the runtime to runtime communication with a peer node"""

    STATUS = enum('PENDING', 'WORKING', 'TERMINATED')

    def __init__(self, links, tunnels, peer_node_id, tunnel_type, policy, rt_id=None, id=None):
        """ links: the calvin networks dictionary of links
            peer_node_id: the id of the peer that we use
            tunnel_type: what is the usage of the tunnel
            policy: TODO not used currently
            id: Tunnel objects on both nodes will use the same id number hence only supply if provided from other side
        """
        super(CalvinTunnel, self).__init__()
        # The tunnel only use one link (links[peer_node_id]) at a time but it can switch at any point
        self.links = links
        self.tunnels = tunnels
        self.peer_node_id = peer_node_id
        self.tunnel_type = tunnel_type
        self.policy = policy
        self.rt_id = rt_id
        # id may change while status is PENDING, but is fixed in WORKING
        self.id = id if id else calvinuuid.uuid("TUNNEL")
        # If id supplied then we must be the second end and hence working
        self.status = CalvinTunnel.STATUS.WORKING if id else CalvinTunnel.STATUS.PENDING
        # Add the tunnel to the dictionary
        if self.peer_node_id:
            if self.peer_node_id in self.tunnels:
                self.tunnels[self.peer_node_id][self.id]=self
            else:
                self.tunnels[self.peer_node_id] = {self.id: self}
        # The callbacks recv for incoming message, down for tunnel failed or died, up for tunnel working
        self.recv_handler = None
        self.down_handler = None
        self.up_handler = None

    def _late_link(self, peer_node_id):
        """ Sometimes the peer is unknown even when a tunnel object is needed.
             This method is called when we have the peer node id.
        """
        self.peer_node_id = peer_node_id
        if self.peer_node_id:
            if self.peer_node_id in self.tunnels:
                self.tunnels[self.peer_node_id][self.id]=self
            else:
                self.tunnels[self.peer_node_id] = {self.id: self}

    def _update_id(self, id):
        """ While status PENDING we might have a simulataneous tunnel setup
            from both sides. The tunnel with highest id is used, this is
            solved by changing the id for the existing tunnel object with
            this method instead of creating a new and destroying the other.
        """
        old_id = self.id
        self.id = id
        self.tunnels[self.peer_node_id].pop(old_id)
        self.tunnels[self.peer_node_id][self.id] = self

    def _setup_ack(self, reply):
        """ Gets called when the tunnel request is acknowledged by the other side """
        if reply and reply.data['tunnel_id'] != self.id:
            self._update_id(reply.data['tunnel_id'])
        if reply:
            self.status = CalvinTunnel.STATUS.WORKING
            if self.up_handler:
                self.up_handler()
        else:
            self.status = CalvinTunnel.STATUS.TERMINATED
            if self.down_handler:
                self.down_handler()

    def _destroy_ack(self, reply):
        """ Gets called when the tunnel destruction is acknowledged by the other side """
        self.close(local_only=True)
        if not reply:
            _log.error("Got none ack on destruction of tunnel!\n%s" % reply)

    def send(self, payload):
        """ Send a payload over the tunnel
            payload must be serializable, i.e. only built-in types such as:
            dict, list, tuple, string, numbers, booleans, etc
        """
        msg = {'cmd': 'TUNNEL_DATA', 'value': payload, 'tunnel_id': self.id}
        try:
            self.links[self.peer_node_id].send(msg)
        except Exception as e:
            # FIXME we failed sending should resend after establishing the link if our node is not quiting
            # so far only seen during node quit
            _log.analyze(self.rt_id, "+ TUNNEL FAILED", payload, peer_node_id=self.peer_node_id)

    def register_recv(self, handler):
        """ Register the handler of incoming messages on this tunnel """
        self.recv_handler = handler

    def register_tunnel_down(self, handler):
        """ Register the handler of tunnel down"""
        self.down_handler = handler

    def register_tunnel_up(self, handler):
        """ Register the handler of tunnel up"""
        self.up_handler = handler

    def close(self, local_only=False):
        """ Removes the tunnel but does not inform
            other end when local_only.

            Currently does not support local_only == False
        """
        self.status = CalvinTunnel.STATUS.TERMINATED
        self.tunnels[self.peer_node_id].pop(self.id)
        if not local_only:
            #FIXME use the tunnel_destroy cmd directly instead
            raise NotImplementedError()

class CalvinProto(CalvinCBClass):
    """ CalvinProto class is the interface between runtimes for all runtime
        subsystem that need to interact. It uses the links in network.

        Besides handling tunnel setup etc, it mainly formats commands uniformerly.
    """

    def __init__(self, node, network, tunnel_handlers=None):
        super(CalvinProto, self).__init__({
            # The commands in messages is called using the CalvinCBClass.
            # Hence it is possible for others to register additional
            # functions that should be called. Either permanent here
            # or using the callback_register method.
            'ACTOR_NEW': [CalvinCB(self.actor_new_handler)],
            'ACTOR_MIGRATE': [CalvinCB(self.actor_migrate_handler)],
            'ACTOR_REPLICATE': [CalvinCB(self.actor_replication_handler)],
            'ACTOR_REPLICATION_REQUEST' : [CalvinCB(self.actor_replication_request_handler)],
            'ACTOR_DESTROY' : [CalvinCB(self.actor_destroy_handler)],
            'APP_DESTROY': [CalvinCB(self.app_destroy_handler)],
            'PORT_CONNECT': [CalvinCB(self.port_connect_handler)],
            'PORT_DISCONNECT': [CalvinCB(self.port_disconnect_handler)],
            'PORT_PENDING_MIGRATE': [CalvinCB(self.not_impl_handler)],
            'PORT_COMMIT_MIGRATE': [CalvinCB(self.not_impl_handler)],
            'PORT_CANCEL_MIGRATE': [CalvinCB(self.not_impl_handler)],
            'PEER_SETUP': [CalvinCB(self.peer_setup_handler)],
            'LOST_NODE': [CalvinCB(self.lost_node_handler)],
            'TUNNEL_NEW': [CalvinCB(self.tunnel_new_handler)],
            'TUNNEL_DESTROY': [CalvinCB(self.tunnel_destroy_handler)],
            'TUNNEL_DATA': [CalvinCB(self.tunnel_data_handler)],
            'REPORT_USAGE': [CalvinCB(self.report_usage_handler)],
            'SEND_RM_INFO': [CalvinCB(self.send_rm_info_handler)],
            'REPLY': [CalvinCB(self.reply_handler)]})

        self.rt_id = node.id
        self.node = node
        self.network = network
        # Register the function that receives all incoming messages
        self.network.register_recv(CalvinCB(self.recv_handler))
        # tunnel_handlers is a dict with key: tunnel_type string e.g. 'token', value: function that get request
        self.tunnel_handlers = tunnel_handlers if isinstance(tunnel_handlers, dict) else {}
        self.tunnels = {}  # key: peer node id, value: dict with key: tunnel_id, value: tunnel obj

    #
    # Reception of incoming payload
    #

    def not_impl_handler(self, payload):
        raise NotImplementedError()

    def reply_handler(self, payload):
        """ Map to specified link's reply_handler"""
        self.network.links[payload['from_rt_uuid']].reply_handler(payload)

    def recv_handler(self, tp_link, payload):
        """ Called by transport when a full payload has been received
        """
        _log.analyze(self.rt_id, "RECV", payload)
        try:
            self.network.link_check(payload['from_rt_uuid'])
        except:
            raise Exception("ERROR_UNKNOWN_RUNTIME")

        if not ('cmd' in payload and payload['cmd'] in self.callback_valid_names()):
            raise Exception("ERROR_UNKOWN_COMMAND")
        # Call the proper handler for the command using CalvinCBClass
        self._callback_execute(payload['cmd'], payload)

    #
    # Remote commands supported by protocol
    #

    #### ACTORS ####

    def actor_new(self, to_rt_uuid, callback, actor_type, state, prev_connections, app_id):
        """ Creates a new actor on to_rt_uuid node, but is only intended for migrating actors
            callback: called when finished with the peers respons as argument
            actor_type: see actor manager
            state: see actor manager
            prev_connections: see actor manager
        """
        if self.node.network.link_request(to_rt_uuid, CalvinCB(self._actor_new,
                                                        to_rt_uuid=to_rt_uuid,
                                                        callback=callback,
                                                        actor_type=actor_type,
                                                        state=state,
                                                        prev_connections=prev_connections,
                                                        app_id=app_id)):
            # Already have link just continue in _actor_new
                self._actor_new(to_rt_uuid, callback, actor_type, state, prev_connections, app_id=app_id, status=response.CalvinResponse(True))

    def _actor_new(self, to_rt_uuid, callback, actor_type, state, prev_connections, status, app_id, peer_node_id=None, uri=None):
        """ Got link? continue actor new """
        if status:
            msg = {'cmd': 'ACTOR_NEW',
                   'state': {
                       'actor_type': actor_type,
                       'actor_state': state,
                       'prev_connections': prev_connections,
                       'app_id': app_id
                   }}
            self.network.links[to_rt_uuid].send_with_reply(callback, msg)
        elif callback:
            callback(status=status)

    def actor_new_handler(self, payload):
        """ Peer request new actor with state and connections """
        _log.analyze(self.rt_id, "+", payload, tb=True)
        self.node.am.new(payload['state']['actor_type'],
                         None,
                         payload['state']['actor_state'],
                         payload['state']['prev_connections'],
                         app_id=payload['state']['app_id'],
                         callback=CalvinCB(self._actor_new_handler, payload))

    def _actor_new_handler(self, payload, status, **kwargs):
        """ Potentially created actor, reply to requesting node """
        msg = {'cmd': 'REPLY', 'msg_uuid': payload['msg_uuid'], 'value': status.encode()}
        self.network.links[payload['from_rt_uuid']].send(msg)

    def actor_replication(self, to_rt_uuid, callback, actor_type, state, prev_connections, args, app_id, start_time):
        """ Replicates a new actor on to_rt_uuid node, but is only intended for migrating actors
            callback: called when finished with the peers respons as argument
            actor_type: see actor manager
            state: see actor manager
            prev_connections: see actor manager
        """
        _log.analyze(self.rt_id, "+", "Replicate actor of type {} to {}".format(actor_type, to_rt_uuid))
        if self.node.network.link_request(to_rt_uuid, CalvinCB(self._actor_replication,
                                                               to_rt_uuid=to_rt_uuid,
                                                               callback=callback,
                                                               actor_type=actor_type,
                                                               state=state,
                                                               prev_connections=prev_connections,
                                                               actor_args=args,
                                                               app_id=app_id,
                                                               start_time=start_time), timeout=0.5):
            # Already have link just continue in _actor_replication
                self._actor_replication(to_rt_uuid, callback, actor_type, state, prev_connections, args, app_id, start_time,
                                        response.CalvinResponse(True))

    def _actor_replication(self, to_rt_uuid, callback, actor_type, state, prev_connections, actor_args, app_id, start_time,
                           status, *args, **kwargs):
        """ Got link? continue actor replication """
        prev_connections['inports'] = [dict(conn) for conn in prev_connections['inports']]
        prev_connections['outports'] = [dict(conn) for conn in prev_connections['outports']]
        if status:
            msg = {'cmd': 'ACTOR_REPLICATE',
                   'state': {
                       'actor_type': actor_type,
                       'prev_connections': prev_connections,
                       'actor_state': state,
                       'app_id': app_id
                   },
                   'args': actor_args,
                   'start_time': start_time}
            self.network.links[to_rt_uuid].send_with_reply(callback, msg, timeout=0.5, replicate=True)
        elif callback:
            callback(status=status)

    def actor_replication_handler(self, payload):
        """ Peer request new actor with state and connections """
        if self.node.storage_node:
            return self._actor_replication_handler(payload, status=response.CalvinResponse(False, data={}))

        _log.analyze(self.rt_id, "+", "Handle replication request {}".format(payload))

        rec_req = time.time() - payload['start_time']
        print "TIME, received request: ", rec_req
        payload['rec_req_time'] = rec_req

        self.node.am.new_replica(payload['state']['actor_type'],
                                 payload['args'],
                                 state=payload['state']['actor_state'],
                                 prev_connections=payload['state']['prev_connections'],
                                 app_id=payload['state']['app_id'],
                                 callback=CalvinCB(self._actor_replication_handler, payload))

    def _actor_replication_handler(self, payload, status, *args, **kwargs):
        """ Potentially created actor, reply to requesting node """
        send_rep = time.time() - payload['start_time']
        status.data['rec_req_time'] = payload['rec_req_time']
        status.data['send_reply_time'] = send_rep
        value = status.encode()
        msg = {
            'cmd': 'REPLY',
            'msg_uuid': payload['msg_uuid'],
            'value': value,
        }
        try:
            self.network.links[payload['from_rt_uuid']].send(msg)
        except:
            _log.warning("Failed to send replication reply")
            pass

    def actor_replication_request(self, actor_id, from_node_id, to_node_id, replication_time, callback):
        """
        Sends a replication request that actor actor_id should be replicated from
        from_node_id to to_node_id
        """
        _log.analyze(self.rt_id, "+", "Request replication of actor {} from {} to {}".format(actor_id, from_node_id, to_node_id))
        if self.node.network.link_request(from_node_id, CalvinCB(self._actor_replication_request,
                                                                 actor_id=actor_id,
                                                                 from_node_id=from_node_id,
                                                                 to_node_id=to_node_id,
                                                                 replication_time=replication_time,
                                                                 callback=callback), timeout=0.2):
            # Already have link just continue in _actor_replication
                self._actor_replication_request(actor_id, from_node_id, to_node_id, replication_time,
                                                callback, status=response.CalvinResponse(True))

    def _actor_replication_request(self, actor_id, from_node_id, to_node_id, replication_time, callback, status, *args, **kwargs):
        """ Got link? continue actor replication request """
        _log.debug("Sending actor replication request of 'actor {} to node {}' from node {} to node {}".format(
            actor_id, to_node_id, self.node.id, from_node_id))
        replication_time = replication_time if replication_time else 1.0
        if status:
            msg = {'cmd': 'ACTOR_REPLICATION_REQUEST',
                   'actor_id': actor_id,
                   'from_node_id': from_node_id,
                   'to_node_id': to_node_id}
            try:
                self.network.links[from_node_id].send_with_reply(callback, msg, timeout=1.5*replication_time)
            except KeyError as e:
                _log.error("Failed to send actor replication request to: {} - {}".format(from_node_id, e))
                if callback:
                    callback(status=response.CalvinResponse(False))
        elif callback:
            _log.warning("Failed to send actor replication request to: {} - {}".format(from_node_id, status))
            callback(status=status)

    def actor_replication_request_handler(self, payload):
        """ Another node requested a replication of an actor"""
        from_node = self.node.resource_manager.node_uris.get(payload['from_node_id'])
        to_node = self.node.resource_manager.node_uris.get(payload['to_node_id'])
        _log.debug("Got replication request from {} to send to {}".format(from_node, to_node))
        _log.analyze(self.rt_id, "+", "Handle of request of a replication request {}".format(payload))
        self.node.am.replicate(payload['actor_id'], payload['to_node_id'], callback=CalvinCB(self._actor_replication_request_handler, payload))

    def _actor_replication_request_handler(self, payload, status, *args, **kwargs):
        """Potentially requested a successfull replication"""
        if not status.data:
            status = response.CalvinResponse(status=response.CalvinResponse(False))

        msg = {'cmd': 'REPLY', 'msg_uuid': payload['msg_uuid'], 'value': status.encode()}
        try:
            self.network.links[payload['from_rt_uuid']].send(msg)
        except Exception as e:
            _log.warning("Failed to reply to actor replication request sent from node {}".format(payload['from_rt_uuid']))

    def actor_migrate(self, to_rt_uuid, callback, actor_id, requirements, extend=False, move=False):
        """ Request actor on to_rt_uuid node to migrate accoring to new deployment requirements
            callback: called when finished with the status respons as argument
            actor_id: actor_id to migrate
            requirements: see app manager
            extend: if extending current deployment requirements
            move: if prefers to move from node
        """
        if self.node.network.link_request(to_rt_uuid, CalvinCB(self._actor_migrate,
                                                        to_rt_uuid=to_rt_uuid,
                                                        callback=callback,
                                                        actor_id=actor_id,
                                                        requirements=requirements,
                                                        extend=extend,
                                                        move=move)):
            # Already have link just continue in _actor_new
                self._actor_migrate(to_rt_uuid, callback, actor_id, requirements,
                                    extend, move, status=response.CalvinResponse(True))

    def _actor_migrate(self, to_rt_uuid, callback, actor_id, requirements, extend, move, status,
                       peer_node_id=None, uri=None):
        """ Got link? continue actor migrate """
        if status:
            msg = {'cmd': 'ACTOR_MIGRATE',
                   'requirements': requirements, 'actor_id': actor_id, 'extend': extend, 'move': move}
            self.network.links[to_rt_uuid].send_with_reply(callback, msg)
        elif callback:
            callback(status=status)

    def actor_migrate_handler(self, payload):
        """ Peer request new actor with state and connections """
        self.node.am.update_requirements(payload['actor_id'], payload['requirements'],
                                         payload['extend'], payload['move'],
                                         callback=CalvinCB(self._actor_migrate_handler, payload))

    def _actor_migrate_handler(self, payload, status, **kwargs):
        """ Potentially migrated actor, reply to requesting node """
        msg = {'cmd': 'REPLY', 'msg_uuid': payload['msg_uuid'], 'value': status.encode()}
        self.network.links[payload['from_rt_uuid']].send(msg)

    def actor_destroy(self, to_rt_uuid, callback, actor_id):
        """ Destroys an actor on to_rt_uuid node
            actor_id: id of the actor
            callback: called when finished with the peer's respons as argument
        """
        if self.network.link_request(to_rt_uuid):
            # Already have link just continue in _actor_destroy
            self._actor_destroy(to_rt_uuid, callback, actor_id, status=response.CalvinResponse(True))
        else:
            # Request link before continue in _app_destroy
            self.node.network.link_request(to_rt_uuid, CalvinCB(self._actor_destroy,
                                                                to_rt_uuid=to_rt_uuid,
                                                                callback=callback,
                                                                actor_id=actor_id), timeout=0.3)

    def _actor_destroy(self, to_rt_uuid, callback, actor_id, status, peer_node_id=None, uri=None):
        """ Got link? continue actor destruction """
        if status:
            msg = {'cmd': 'ACTOR_DESTROY', 'actor_id':actor_id}
            self.network.links[to_rt_uuid].send_with_reply(callback, msg, timeout=0.3)
        elif callback:
            callback(status=status)

    def actor_destroy_handler(self, payload):
        """ Peer requested deletion of actor
        """
        try:
            self.node.am.delete_actor(payload['actor_id'], delete_from_app=True)
            reply = response.CalvinResponse(response.OK)
        except:
            _log.exception("Destroy actor failed")
            reply = response.CalvinResponse(response.NOT_FOUND)
        msg = {'cmd': 'REPLY', 'msg_uuid': payload['msg_uuid'], 'value': reply.encode()}
        self.network.links[payload['from_rt_uuid']].send(msg)

    #### APPS ####

    def app_destroy(self, to_rt_uuid, callback, app_id, actor_ids):
        """ Destroys an application with remote actors on to_rt_uuid node
            callback: called when finished with the peer's respons as argument
            app_id: the application to destroy
            actor_ids: optional list of actors to destroy
        """
        if self.network.link_request(to_rt_uuid):
            # Already have link just continue in _app_destroy
            self._app_destroy(to_rt_uuid, callback, app_id, actor_ids, status=response.CalvinResponse(True))
        else:
            # Request link before continue in _app_destroy
            self.node.network.link_request(to_rt_uuid, CalvinCB(self._app_destroy,
                                                        to_rt_uuid=to_rt_uuid,
                                                        callback=callback,
                                                        app_id=app_id,
                                                        actor_ids=actor_ids))

    def _app_destroy(self, to_rt_uuid, callback, app_id, actor_ids, status, peer_node_id=None, uri=None):
        """ Got link? continue app destruction """
        if status:
            msg = {'cmd': 'APP_DESTROY', 'app_uuid': app_id, 'actor_uuids': actor_ids}
            self.network.links[to_rt_uuid].send_with_reply(callback, msg)
        elif callback:
            callback(status=status)

    def app_destroy_handler(self, payload):
        """ Peer request destruction of app and its actors """
        reply = self.node.app_manager.destroy_request(payload['app_uuid'],
                                                      payload['actor_uuids'] if 'actor_uuids' in payload else [])
        msg = {'cmd': 'REPLY', 'msg_uuid': payload['msg_uuid'], 'value': reply.encode()}
        self.network.links[payload['from_rt_uuid']].send(msg)

    #### TUNNELS ####

    def register_tunnel_handler(self, tunnel_type, handler):
        """ Any users of tunnels need to register a handler for a tunnel_type.
            The handler will be called when a peer request a tunnel.
            tunnel_type: string specify the tunnel usage, e.g. 'token'
            handler: function that takes tunnel as argument and returns True if it accepts and False otherwise
        """
        self.tunnel_handlers[tunnel_type] = handler

    def tunnel_new(self, to_rt_uuid, tunnel_type, policy):
        """ Either create a new tunnel (request side), with status pending,
            or return an existing tunnel with the same tunnel_type
            to_rt_uuid: peer node id
            tunnel_type: tunnel usage string
            policy: Not currently used
        """
        try:
            self.network.link_check(to_rt_uuid)
        except:
            # Need to join the other peer first
            # Create a tunnel object which is not inserted on a link yet
            tunnel = CalvinTunnel(self.network.links, self.tunnels, None, tunnel_type, policy, rt_id=self.node.id)
            self.network.link_request(to_rt_uuid, CalvinCB(self._tunnel_link_request_finished, tunnel=tunnel, to_rt_uuid=to_rt_uuid, tunnel_type=tunnel_type, policy=policy))
            return tunnel

        # Do we have a tunnel already?
        tunnel = self._get_tunnel(to_rt_uuid, tunnel_type = tunnel_type)
        if tunnel != None:
            return tunnel

        # Create new tunnel and send request to peer
        tunnel = CalvinTunnel(self.network.links, self.tunnels, to_rt_uuid, tunnel_type, policy, rt_id=self.node.id)
        self._tunnel_new_msg(tunnel, to_rt_uuid, tunnel_type, policy)
        return tunnel

    def _get_tunnel(self, peer_node_id, tunnel_type=None):
        try:
            return [t for t in self.tunnels[peer_node_id].itervalues() if t.tunnel_type == tunnel_type][0]
        except:
            return None

    def _tunnel_new_msg(self, tunnel, to_rt_uuid, tunnel_type, policy):
        """ Create and send the tunnel new message """
        msg = {'cmd': 'TUNNEL_NEW', 'type': tunnel_type, 'tunnel_id': tunnel.id, 'policy': policy}
        self.network.links[to_rt_uuid].send_with_reply(CalvinCB(tunnel._setup_ack), msg)

    def _tunnel_link_request_finished(self, status, tunnel, to_rt_uuid, tunnel_type, policy, peer_node_id=None, uri=None):
        """ Got a link, now continue with tunnel setup """
        _log.analyze(self.rt_id, "+" , {'status': status.__str__()}, peer_node_id=to_rt_uuid)
        try:
            self.network.link_check(to_rt_uuid)
        except:
            # For some reason we still did not have a link
            raise Exception("ERROR_UNKNOWN_RUNTIME")

        # Set the link and send request for new tunnel
        tunnel._late_link(to_rt_uuid)
        self._tunnel_new_msg(tunnel, to_rt_uuid, tunnel_type, policy)
        return None

    def tunnel_new_handler(self, payload):
        """ Create a new tunnel (response side) """
        tunnel = self._get_tunnel(payload['from_rt_uuid'], payload['type'])
        ok = False
        _log.analyze(self.rt_id, "+", payload, peer_node_id=payload['from_rt_uuid'])
        if tunnel:
            _log.analyze(self.rt_id, "+ PENDING", payload, peer_node_id=payload['from_rt_uuid'])
            # Got tunnel new request while we already have one pending
            # it is not allowed to send new request while a tunnel is working
            if tunnel.status != CalvinTunnel.STATUS.WORKING:
                ok = True
                # The one with lowest tunnel id loose
                if tunnel.id < payload['tunnel_id']:
                    # Our tunnel has lowest id, change our tunnels id
                    # update status and call proper callbacks
                    # but send tunnel reply first, to get everything in order
                    msg = {'cmd': 'REPLY', 'msg_uuid': payload['msg_uuid'], 'value': response.CalvinResponse(ok, data={'tunnel_id': payload['tunnel_id']}).encode()}
                    self.network.links[payload['from_rt_uuid']].send(msg)
                    tunnel._setup_ack(response.CalvinResponse(True, data={'tunnel_id': payload['tunnel_id']}))
                    _log.analyze(self.rt_id, "+ CHANGE ID", payload, peer_node_id=payload['from_rt_uuid'])
                else:
                    # Our tunnel has highest id, keep our id
                    # update status and call proper callbacks
                    # but send tunnel reply first, to get everything in order
                    msg = {'cmd': 'REPLY', 'msg_uuid': payload['msg_uuid'], 'value': response.CalvinResponse(ok, data={'tunnel_id': tunnel.id}).encode()}
                    self.network.links[payload['from_rt_uuid']].send(msg)
                    tunnel._setup_ack(response.CalvinResponse(True, data={'tunnel_id': tunnel.id}))
                    _log.analyze(self.rt_id, "+ KEEP ID", payload, peer_node_id=payload['from_rt_uuid'])
            else:
                # FIXME if this happens need to decide what to do
                _log.analyze(self.rt_id, "+ DROP FIXME", payload, peer_node_id=payload['from_rt_uuid'])
            return
        else:
            # No simultaneous tunnel requests, lets create it...
            tunnel = CalvinTunnel(self.network.links, self.tunnels, payload['from_rt_uuid'], payload['type'], payload['policy'], rt_id=self.node.id, id=payload['tunnel_id'])
            _log.analyze(self.rt_id, "+ NO SMASH", payload, peer_node_id=payload['from_rt_uuid'])
            try:
                # ... and see if the handler wants it
                ok = self.tunnel_handlers[payload['type']](tunnel)
            except:
                pass
        # Send the response
        msg = {'cmd': 'REPLY', 'msg_uuid': payload['msg_uuid'], 'value': response.CalvinResponse(ok, data={'tunnel_id': tunnel.id}).encode()}
        self.network.links[payload['from_rt_uuid']].send(msg)

        # If handler did not want it close it again
        if not ok:
            tunnel.close(local_only=True)

    def tunnel_destroy(self, to_rt_uuid, tunnel_uuid):
        """ Destroy a tunnel (request side) """
        try:
            self.network.link_check(to_rt_uuid)
        except:
            raise Exception("ERROR_UNKNOWN_RUNTIME")
        try:
            tunnel = self.tunnels[to_rt_uuid][tunnel_uuid]
        except:
            raise Exception("ERROR_UNKNOWN_TUNNEL")
            _log.analyze(self.rt_id, "+ ERROR_UNKNOWN_TUNNEL", None)
        # It exist, lets request its destruction
        msg = {'cmd': 'TUNNEL_DESTROY', 'tunnel_id': tunnel.id}
        self.network.links[to_rt_uuid].send_with_reply(CalvinCB(tunnel._destroy_ack), msg)

    def tunnel_destroy_handler(self, payload):
        """ Destroy tunnel (response side) """
        try:
            self.network.link_check(payload['to_rt_uuid'])
        except:
            raise Exception("ERROR_UNKNOWN_RUNTIME")
        try:
            tunnel = self.tunnels[payload['from_rt_uuid']][payload['tunnel_id']]
        except:
            raise Exception("ERROR_UNKNOWN_TUNNEL")
            _log.analyze(self.rt_id, "+ ERROR_UNKNOWN_TUNNEL", payload, peer_node_id=payload['from_rt_uuid'])
        # We have the tunnel so close it
        tunnel.close(local_only=True)
        ok = False
        try:
            # Hope the tunnel don't mind,
            # TODO since the requester likely don't know what to do and we have already closed it
            ok = tunnel.down_handler()
        except:
            pass
        msg = {'cmd': 'REPLY', 'msg_uuid': payload['msg_uuid'], 'value': response.CalvinResponse(ok).encode()}
        self.network.links[payload['from_rt_uuid']].send(msg)

    def tunnel_data_handler(self, payload):
        """ Map received data over tunnel to the correct link and tunnel """
        try:
            tunnel = self.tunnels[payload['from_rt_uuid']][payload['tunnel_id']]
        except:
            _log.analyze(self.rt_id, "+ ERROR_UNKNOWN_TUNNEL", payload, peer_node_id=payload['from_rt_uuid'])
            raise Exception("ERROR_UNKNOWN_TUNNEL")
        try:
            tunnel.recv_handler(payload['value'])
        except Exception as e:
            _log.exception("Check error in tunnel recv handler")
            _log.analyze(self.rt_id, "+ EXCEPTION TUNNEL RECV HANDLER", {'payload': payload, 'exception': str(e)},
                                                                peer_node_id=payload['from_rt_uuid'], tb=True)

    #### PORTS ####

    def port_connect(self, callback=None, port_id=None, peer_node_id=None, peer_port_id=None, peer_actor_id=None,
                     peer_port_name=None, peer_port_dir=None, tunnel=None, port_states=None):
        """ Before calling this method all needed information and when requested a tunnel must be available
            see port manager for parameters
        """
        if tunnel:
            msg = {'cmd': 'PORT_CONNECT', 'port_id': port_id, 'peer_actor_id': peer_actor_id,
                   'peer_port_name': peer_port_name, 'peer_port_id': peer_port_id, 'peer_port_dir': peer_port_dir,
                   'port_states': port_states, 'tunnel_id': tunnel.id}
            cb = CalvinCB(self._port_connect, peer_node_id=peer_node_id, msg=msg, callback=callback)
            if self.node.network.link_request(peer_node_id, cb):
                self._port_connect(peer_node_id=peer_node_id, msg=msg, callback=callback, status=response.CalvinResponse(True))
        else:
            raise NotImplementedError()

    def _port_connect(self, peer_node_id, msg, callback, status, *args, **kwargs):
        if status:
            self.network.links[peer_node_id].send_with_reply(callback, msg, port_connect=True)
        elif callback:
            callback(status=status)

    def port_connect_handler(self, payload):
        """ Request for port connection """
        #print 'port_connect_handler', datetime.now()

        reply = self.node.pm.connection_request(payload)
        # Send reply
        msg = {'cmd': 'REPLY', 'msg_uuid': payload['msg_uuid'], 'value': reply.encode()}
        self.network.links[payload['from_rt_uuid']].send(msg)

    def port_disconnect(self, callback=None, port_id=None, peer_node_id=None, peer_port_id=None, peer_actor_id=None,
                        peer_port_name=None, peer_port_dir=None, tunnel=None):
        """ Before calling this method all needed information must be available
            see port manager for parameters
        """
        if not peer_node_id:
            if callback:
                callback(status=response.CalvinResponse(False))
            return

        cb = CalvinCB(self._port_disconnect, callback=callback, port_id=port_id, peer_node_id=peer_node_id,
                      peer_port_id=peer_port_id, peer_actor_id=peer_actor_id, peer_port_name=peer_port_name,
                      peer_port_dir=peer_port_dir)
        if self.node.network.link_request(peer_node_id, cb):
            # Already have link just continue in _port_disconnect
            self._port_disconnect(callback=callback, port_id=port_id, peer_node_id=peer_node_id,
                                  peer_port_id=peer_port_id, peer_actor_id=peer_actor_id,
                                  peer_port_name=peer_port_name, peer_port_dir=peer_port_dir,
                                  status=response.CalvinResponse(True))

    def _port_disconnect(self, callback, port_id, peer_node_id, peer_port_id, peer_actor_id, peer_port_name,
                         peer_port_dir, status, uri=None):
        if status:
            msg = {'cmd': 'PORT_DISCONNECT', 'port_id': port_id, 'peer_actor_id': peer_actor_id,
                   'peer_port_name': peer_port_name, 'peer_port_id': peer_port_id, 'peer_port_dir': peer_port_dir}
            self.network.links[peer_node_id].send_with_reply(callback, msg)
        elif callback:
            callback(status=status)

    def port_disconnect_handler(self, payload):
        """ Reguest for port disconnect """
        if self.node.network.link_request(payload['to_rt_uuid'], CalvinCB(self._port_disconnect_handler, payload=payload)):
            # Already have link just continue in _port_disconnect-handler
                self._port_disconnect_handler(payload=payload, status=response.CalvinResponse(True))

    def _port_disconnect_handler(self, payload, status=None, peer_node_id=None, uri=None):
        # Send reply
        reply = self.node.pm.disconnection_request(payload)
        msg = {'cmd': 'REPLY', 'msg_uuid': payload['msg_uuid'], 'value': reply.encode()}
        self.network.links[payload['from_rt_uuid']].send(msg)

    #### NODES ####

    def peer_setup(self, to_rt_uuid, peers, callback=None):
        """ Sends a peer setup request to the other node.
            prev_connections: list of node ids to setup a connecting with
        """
        if self.node.network.link_request(to_rt_uuid, CalvinCB(self._peer_setup, to_rt_uuid=to_rt_uuid,
                                                               callback=callback, peers=peers)):
            # Already have link just continue in _peer_setup
                self._peer_setup(to_rt_uuid=to_rt_uuid, callback=callback, peers=peers, status=response.CalvinResponse(True))

    def _peer_setup(self, to_rt_uuid, callback, status, peers=[], uri=None, *args, **kwargs):
        """ Got link? continue actor new """
        if status:
            msg = {'cmd': 'PEER_SETUP',
                   'peers': peers}
            self.network.links[to_rt_uuid].send_with_reply(callback, msg)
        elif callback:
            callback(status=status)

    def peer_setup_handler(self, payload):
        """ Peer request new actor with state and connections """
        self.node.peersetup(payload['peers'], cb=CalvinCB(self._peer_setup_handler, payload))

    def _peer_setup_handler(self, payload, status, **kwargs):
        """ Potentially created actor, reply to requesting node """
        msg = {'cmd': 'REPLY', 'msg_uuid': payload['msg_uuid'], 'value': status.encode()}
        self.network.links[payload['from_rt_uuid']].send(msg)

    def lost_node(self, to_rt_uuid, node_id, callback):
        """ Sends a peer setup request to the other node.
            prev_connections: list of node ids to setup a connecting with
        """
        if self.node.network.link_request(to_rt_uuid, CalvinCB(self._lost_node, to_rt_uuid=to_rt_uuid,
                                                               callback=callback, node_id=node_id), timeout=0.2):
            # Already have link just continue in _peer_setup
                self._lost_node(to_rt_uuid, callback, node_id, status=response.CalvinResponse(True))

    def _lost_node(self, to_rt_uuid, callback, node_id, status, uri=None, *args, **kwargs):
        """ Got link? continue actor new """
        _log.debug("In _lost_node: {}".format(status))
        if status:
            _log.debug("Sending LOST_NODE of node {} to {} - {} with callback {}".format(
                node_id, to_rt_uuid, self.node.resource_manager.node_uris.get(to_rt_uuid), callback))
            msg = {'cmd': 'LOST_NODE',
                   'node_id': node_id}
            self.network.links[to_rt_uuid].send_with_reply(callback, msg, timeout=0.5)
        elif callback:
            _log.error("Failed to send LOST_NODE of node {} to {} - {}".format(node_id, to_rt_uuid, self.node.resource_manager.node_uris.get(to_rt_uuid)))
            callback(status=status)

    def lost_node_handler(self, payload):
        """ Peer request new actor with state and connections """
        from_node = self.node.resource_manager.node_uris.get(payload['from_rt_uuid'])
        node = self.node.resource_manager.node_uris.get(payload['node_id'])
        _log.debug("Handling lost node request of node {} - {} sent from {}".format(payload['node_id'], node, from_node))
        cb = CalvinCB(self._lost_node_handler, payload=payload)
        self.node.lost_node_request(payload['node_id'], cb)

    def _lost_node_handler(self, payload, status, **kwargs):
        """ Potentially created actor, reply to requesting node """
        if isinstance(status, int):
            status = response.CalvinResponse(status)

        if status:
            status = status.encode()
        else:
            _log.error("Failed to handle lost node {}".format(status))
            status = response.CalvinResponse(410)

        msg = {'cmd': 'REPLY', 'msg_uuid': payload['msg_uuid'], 'value': status}
        from_rt_uuid = payload['from_rt_uuid']
        if self.node.network.link_request(from_rt_uuid, CalvinCB(self._lost_node_reply, to_rt_uuid=from_rt_uuid, msg=msg)):
            self._lost_node_reply(to_rt_uuid=from_rt_uuid, msg=msg, status=response.CalvinResponse(True))

    def _lost_node_reply(self, to_rt_uuid, msg, status, **kwargs):
        if status:
            try:
                self.network.links[to_rt_uuid].send(msg)
            except KeyError as e:
                _log.error("Failed to send reply to {} - {}".format(to_rt_uuid, e))
        else:
            from_node = self.node.resource_manager.node_uris.get(to_rt_uuid)
            _log.error("Failed to send reply to {} - {}".format(to_rt_uuid, from_node))

    ### RESOURCE USAGE ###

    def report_usage(self, to_rt_uuid, node_id, usage, callback=None, timeout=0.3):
        if self.node.network.link_request(to_rt_uuid, CalvinCB(self._report_usage, to_rt_uuid, node_id, usage, callback), timeout=timeout):
            # Already have link just continue in _actor_new
            self._report_usage(to_rt_uuid, node_id, usage, callback, response.CalvinResponse(True))

    def _report_usage(self, to_rt_uuid, node_id, usage, callback, status, uri=None, *args, **kwargs):
        """ Got link? continue report usage """
        if status:
            msg = {'cmd': 'REPORT_USAGE',
                   'node_id': node_id,
                   'usage': usage}
            try:
                self.network.links[to_rt_uuid].send_with_reply(callback, msg, timeout=0.3)
            except KeyError as e:
                if callback:
                    callback(status=response.CalvinResponse(False))
        elif callback:
            callback(status=status)

    def report_usage_handler(self, payload):
        """ Peer reported new usage """
        _log.analyze(self.rt_id, "+", payload, tb=True)
        self.node.register_resource_usage(payload['node_id'], payload['usage'],
                                          callback=CalvinCB(self._report_usage_handler, payload))

    def _report_usage_handler(self, payload, status, **kwargs):
        msg = {'cmd': 'REPLY', 'msg_uuid': payload['msg_uuid'], 'value': status.encode()}
        self.network.links[payload['from_rt_uuid']].send(msg)

    ### Synchronize resource manager information ###

    def send_rm_info(self, to_rt_uuid, usages, callback):
        if self.node.network.link_request(to_rt_uuid, CalvinCB(self._send_rm_info, to_rt_uuid, usages, callback)):
            # Already have link just continue in _actor_new
            self._send_rm_info(to_rt_uuid, usages, callback, response.CalvinResponse(True))

    def _send_rm_info(self, to_rt_uuid, usages, callback, status, *args, **kwargs):
        """ Got link? continue send rm info """
        if status:
            msg = {'cmd': 'SEND_RM_INFO',
                    'usages':usages}
            try:
                self.network.links[to_rt_uuid].send_with_reply(callback, msg)
            except KeyError as e:
                if callback:
                    callback(status=response.CalvinResponse(False))
        elif callback:
            callback(status=status)

    def send_rm_info_handler(self, payload):
        """ Peer reported new replication time """
        _log.analyze(self.rt_id, "+", payload, tb=True)
        self.node.sync_rm_info(payload['usages'], callback=CalvinCB(self._send_rm_info_handler, payload))

    def _send_rm_info_handler(self, payload, usages, status, **kwargs):
        value = status.encode()
        value['data'] = [usages]
        msg = {'cmd': 'REPLY', 'msg_uuid': payload['msg_uuid'], 'value': value}
        self.network.links[payload['from_rt_uuid']].send(msg)


if __name__ == '__main__':
    import pytest
    pytest.main("-vs runtime/north/tests/test_calvin_proto.py")
