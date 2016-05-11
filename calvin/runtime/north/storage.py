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

import time
import re
import random

from calvin.runtime.north.plugins.storage import storage_factory
from calvin.runtime.north.plugins.coders.messages import message_coder_factory
from calvin.runtime.south.plugins.async import async
from calvin.utilities import calvinuuid
from calvin.utilities import calvinlogger
from calvin.utilities.calvin_callback import CalvinCB
from calvin.actor import actorport
from calvin.actor.actor import ShadowActor
from calvin.utilities import calvinconfig
from calvin.actorstore.store import GlobalStore
from calvin.utilities import dynops

_log = calvinlogger.get_logger(__name__)
_conf = calvinconfig.get()

class Storage(object):

    """
    Storage helper functions.
    All functions in this class should be async and never block.
    """

    def __init__(self, node):
        self.localstore = {}
        self.localstore_sets = {}
        self.started = False
        self.node = node
        self.proxy = _conf.get(None, 'storage_proxy')
        _log.analyze(self.node.id, "+", {'proxy': self.proxy})
        self.tunnel = {}
        self.starting = _conf.get(None, 'storage_start')
        self.storage = storage_factory.get("proxy" if self.proxy else "dht", node) if self.starting else None
        self.coder = message_coder_factory.get("json")  # TODO: always json? append/remove requires json at the moment
        self.flush_delayedcall = None
        self.reset_flush_timeout()

    ### Storage life cycle management ###

    def reset_flush_timeout(self):
        """ Reset flush timeout
        """
        self.flush_timeout = 0.2

    def trigger_flush(self, delay=None):
        """ Trigger a flush of internal data
        """
        if self.localstore or self.localstore_sets:
            if delay is None:
                delay = self.flush_timeout
            if self.flush_delayedcall is not None:
                self.flush_delayedcall.cancel()
            self.flush_delayedcall = async.DelayedCall(delay, self.flush_localdata)

    def flush_localdata(self):
        """ Write data in localstore to storage
        """
        _log.debug("Flush local storage data")
        if self.flush_timeout < 600:
            self.flush_timeout = self.flush_timeout * 2
        self.flush_delayedcall = None
        for key in self.localstore:
            _log.debug("Flush key %s: %s" % (key, self.localstore[key]))
            self.storage.set(key=key, value=self.localstore[key],
                             cb=CalvinCB(func=self.set_cb, org_key=None, org_value=None, org_cb=None))

        for key, value in self.localstore_sets.iteritems():
            self._flush_append(key, value['+'])
            self._flush_remove(key, value['-'])

    def _flush_append(self, key, value):
        if not value:
            return

        _log.debug("Flush append on key %s: %s" % (key, list(value)))
        coded_value = self.coder.encode(list(value))
        self.storage.append(key=key, value=coded_value,
                            cb=CalvinCB(func=self.append_cb, org_key=None, org_value=None, org_cb=None))

    def _flush_remove(self, key, value):
        if not value:
            return

        _log.debug("Flush remove on key %s: %s" % (key, list(value)))
        coded_value = self.coder.encode(list(value))
        self.storage.remove(key=key, value=coded_value,
                            cb=CalvinCB(func=self.remove_cb, org_key=None, org_value=None, org_cb=None))

    def started_cb(self, *args, **kwargs):
        """ Called when storage has started, flushes localstore
        """
        if not args[0]:
            return

        self.started = True
        self.trigger_flush(0)
        if kwargs["org_cb"]:
            async.DelayedCall(0, kwargs["org_cb"], args[0])

    def start(self, iface='', cb=None):
        """ Start storage
        """
        _log.analyze(self.node.id, "+", None)
        if self.starting:
            self.storage.start(iface=iface, cb=CalvinCB(self.started_cb, org_cb=cb))

        if not self.proxy:
            self._init_proxy()

    def _init_proxy(self):
	_log.debug("Storage proxy INIT!")
        _log.analyze(self.node.id, "+ SERVER", None)
        # We are not proxy client, so we can be proxy bridge/master
        self._proxy_cmds = {'GET': self.get,
                            'SET': self.set,
                            'GET_CONCAT': self.get_concat,
                            'APPEND': self.append,
                            'REMOVE': self.remove,
                            'DELETE': self.delete,
                            'REPLY': self._proxy_reply}
        try:
            self.node.proto.register_tunnel_handler('storage', CalvinCB(self.tunnel_request_handles))
        except:
            # OK, then skip being a proxy server
            pass

    def stop(self, cb=None):
        """ Stop storage
        """
        _log.analyze(self.node.id, "+", {'started': self.started})
        if self.started:
            self.storage.stop(cb=cb)
        elif cb:
            cb()
        self.started = False

    ### Storage operations ###

    def set_cb(self, key, value, org_key, org_value, org_cb):
        """ set callback, on error store in localstore and retry after flush_timeout
        """
        if value:
            if org_cb:
                org_cb(key=key, value=True)
            if key in self.localstore:
                del self.localstore[key]
            self.reset_flush_timeout()
        else:
            _log.error("Failed to store %s" % key)
            if org_key and org_value:
                if not org_value is None:
                    self.localstore[key] = org_value
            if org_cb:
                org_cb(key=key, value=False)

        self.trigger_flush()

    def set(self, prefix, key, value, cb):
        """ Set key: prefix+key value: value
        """
        _log.debug("Set key %s, value %s" % (prefix + key, value))
        value = self.coder.encode(value) if value else value
        if prefix + key in self.localstore_sets:
            del self.localstore_sets[prefix + key]

        # Always save locally
        self.localstore[prefix + key] = value

        if self.started:
            self.storage.set(key=prefix + key, value=value, cb=CalvinCB(func=self.set_cb, org_key=key, org_value=value, org_cb=cb))
        elif cb:
            async.DelayedCall(0, cb, key=key, value=True)

    def get_cb(self, key, value, org_cb, org_key):
        """ get callback
        """
        if value:
            value = self.coder.decode(value)
        org_cb(org_key, value)

    def get(self, prefix, key, cb):
        """ Get value for key: prefix+key, first look in localstore
        """
        _log.debug("Getting from storage: {}{}".format(prefix, key))
        if not cb:
            return

        if prefix + key in self.localstore:
            value = self.localstore[prefix + key]
            if value:
                value = self.coder.decode(value)
            async.DelayedCall(0, cb, key=key, value=value)
        else:
            try:
                self.storage.get(key=prefix + key, cb=CalvinCB(func=self.get_cb, org_cb=cb, org_key=key))
            except:
                _log.error("Failed to get: %s" % key)
                async.DelayedCall(0, cb, key=key, value=False)

    def get_iter_cb(self, key, value, it, org_key, include_key=False):
        """ get callback
        """
        _log.analyze(self.node.id, "+ BEGIN", {'value': value, 'key': org_key})
        if value:
            value = self.coder.decode(value)
            it.append((key, value) if include_key else value)
            _log.analyze(self.node.id, "+", {'value': value, 'key': org_key})
        else:
            _log.analyze(self.node.id, "+", {'value': 'FailedElement', 'key': org_key})
            it.append((key, dynops.FailedElement) if include_key else dynops.FailedElement)

    def get_iter(self, prefix, key, it, include_key=False):
        """ Get value for key: prefix+key, first look in localstore
            Add the value to the supplied dynamic iterable (preferable a LimitedList or List)
        """
        if it:
            if prefix + key in self.localstore:
                value = self.localstore[prefix + key]
                if value:
                    value = self.coder.decode(value)
                _log.analyze(self.node.id, "+", {'value': value, 'key': key})
                it.append((key, value) if include_key else value)
            else:
                try:
                    self.storage.get(key=prefix + key,
                                     cb=CalvinCB(func=self.get_iter_cb, it=it, org_key=key, include_key=include_key))
                except:
                    _log.analyze(self.node.id, "+", {'value': 'FailedElement', 'key': key})
                    _log.error("Failed to get: %s" % key)
                    it.append((key, dynops.FailedElement) if include_key else dynops.FailedElement)

    def get_concat_cb(self, key, value, org_cb, org_key, local_list):
        """ get callback
        """
        if value:
            value = self.coder.decode(value)
            value = value if value else []
            org_cb(org_key, list(set(value + local_list)))
        else:
            org_cb(org_key, local_list if local_list else None)

    def get_concat(self, prefix, key, cb):
        """ Get value for key: prefix+key, first look in localstore
            Return value is list. The storage could be eventually consistent.
            For example a remove might only have reached part of the
            storage and hence the return list might contain removed items,
            but also missing items.
        """
        if not cb:
            return

        if prefix + key in self.localstore_sets:
            _log.analyze(self.node.id, "+ GET LOCAL", None)
            value = self.localstore_sets[prefix + key]
            # Return the set that we intended to append since that's all we have until it is synced
            local_list = list(value['+'])
        else:
            local_list = []
        try:
            self.storage.get_concat(key=prefix + key,
                                    cb=CalvinCB(func=self.get_concat_cb, org_cb=cb, org_key=key, local_list=local_list))
        except:
            _log.error("Failed to get: %s" % key, exc_info=True)
            async.DelayedCall(0, cb, key=key, value=local_list if local_list else None)

    def get_concat_iter_cb(self, key, value, org_key, include_key, it):
        """ get callback
        """
        _log.analyze(self.node.id, "+ BEGIN", {'key': org_key, 'value': value, 'iter': str(it)})
        if value:
            value = self.coder.decode(value)
            _log.analyze(self.node.id, "+ VALUE", {'value': value, 'key': org_key})
            it.extend([(org_key, v) for v in value] if include_key else value)
        it.final()
        _log.analyze(self.node.id, "+ END", {'key': org_key, 'iter': str(it)})

    def get_concat_iter(self, prefix, key, include_key=False):
        """ Get value for key: prefix+key, first look in localstore
            Returned value is dynamic iterable. The storage could be eventually consistent.
            For example a remove might only have reached part of the
            storage and hence the return iterable might contain removed items,
            but also missing items.
        """
        _log.analyze(self.node.id, "+ BEGIN", {'key': key})
        if prefix + key in self.localstore_sets:
            _log.analyze(self.node.id, "+ GET LOCAL", None)
            value = self.localstore_sets[prefix + key]
            # Return the set that we intended to append since that's all we have until it is synced
            local_list = list(value['+'])
            _log.analyze(self.node.id, "+", {'value': local_list, 'key': key})
        else:
            local_list = []
        if include_key:
            local_list = [(key, v) for v in local_list]
        it = dynops.List(local_list)
        try:
            self.storage.get_concat(key=prefix + key,
                            cb=CalvinCB(func=self.get_concat_iter_cb, org_key=key,
                                        include_key=include_key, it=it))
        except:
            if self.started:
                _log.error("Failed to get: %s" % key, exc_info=True)
            it.final()
        _log.analyze(self.node.id, "+ END", {'key': key, 'iter': str(it)})
        return it

    def append_cb(self, key, value, org_key, org_value, org_cb):
        """ append callback, on error retry after flush_timeout
        """
        if value:
            if org_cb:
                org_cb(key=org_key, value=True)
            if key in self.localstore_sets:
                if self.localstore_sets[key]['-']:
                    self.localstore_sets[key]['+'] = set([])
                else:
                    del self.localstore_sets[key]
                self.reset_flush_timeout()
        else:
            _log.error("Failed to update %s" % key)
            if org_cb:
                org_cb(key=org_key, value=False)

        self.trigger_flush()

    def append(self, prefix, key, value, cb):
        """ set operation append on key: prefix+key value: value is a list of items
        """
        _log.debug("Append key %s, value %s" % (prefix + key, value))
        # Keep local storage for sets updated until confirmed
        if (prefix + key) in self.localstore_sets:
            # Append value items
            self.localstore_sets[prefix + key]['+'] |= set(value)
            # Don't remove value items any more
            self.localstore_sets[prefix + key]['-'] -= set(value)
        else:
            self.localstore_sets[prefix + key] = {'+': set(value), '-': set([])}

        if self.started:
            coded_value = self.coder.encode(list(self.localstore_sets[prefix + key]['+']))
            self.storage.append(key=prefix + key, value=coded_value,
                                cb=CalvinCB(func=self.append_cb, org_key=key, org_value=value, org_cb=cb))
        else:
            if cb:
                cb(key=key, value=True)

    def remove_cb(self, key, value, org_key, org_value, org_cb):
        """ remove callback, on error retry after flush_timeout
        """
        if value:
            if org_cb:
                org_cb(key=org_key, value=True)
            if key in self.localstore_sets:
                if self.localstore_sets[key]['+']:
                    self.localstore_sets[key]['-'] = set([])
                else:
                    del self.localstore_sets[key]
            self.reset_flush_timeout()
        else:
            _log.error("Failed to update %s" % key)
            if org_cb:
                org_cb(key=org_key, value=False)

        self.trigger_flush()

    def remove(self, prefix, key, value, cb):
        """ set operation remove on key: prefix+key value: value is a list of items
        """
        _log.debug("Remove key %s, value %s" % (prefix + key, value))
        # Keep local storage for sets updated until confirmed
        if (prefix + key) in self.localstore_sets:
            # Don't append value items any more
            self.localstore_sets[prefix + key]['+'] -= set(value)
            # Remove value items
            self.localstore_sets[prefix + key]['-'] |= set(value)
        else:
            self.localstore_sets[prefix + key] = {'+': set([]), '-': set(value)}

        if self.started:
            coded_value = self.coder.encode(list(self.localstore_sets[prefix + key]['-']))
            self.storage.remove(key=prefix + key, value=coded_value,
                                cb=CalvinCB(func=self.remove_cb, org_key=key, org_value=value, org_cb=cb))
        elif cb:
            cb(key=key, value=True)

    def delete(self, prefix, key, cb):
        """ Delete key: prefix+key (value set to None)
        """
        _log.debug("Deleting key %s" % prefix + key)
        if prefix + key in self.localstore:
            del self.localstore[prefix + key]
        if (prefix + key) in self.localstore_sets:
            del self.localstore_sets[prefix + key]
        if self.started:
            self.set(prefix, key, None, cb)
        else:
            if cb:
                cb(key, True)

    ### Calvin object handling ###

    def add_node(self, node, cb=None):
        """
        Add node to storage
        """
        self.set(prefix="node-", key=node.id,
                 value={"uri": node.uri,
                        "control_uri": node.control_uri,
                        "attributes": {'public': node.attributes.get_public(),
                                       'indexed_public': node.attributes.get_indexed_public(as_list=False)}}, cb=cb)
        self._add_node_index(node)
        # Store all actors on this node in storage
        GlobalStore(node=node).export()

    def _add_node_index(self, node, cb=None):
        indexes = node.attributes.get_indexed_public()
        try:
            for index in indexes:
                # TODO add callback, but currently no users supply a cb anyway
                self.add_index(index, node.id)
        except:
            _log.debug("Add node index failed", exc_info=True)
            pass
        # Add the capabilities
        try:
            for c in node._calvinsys.list_capabilities():
                self.add_index(['node', 'capabilities', c], node.id, root_prefix_level=3)
        except:
            _log.debug("Add node capabilities failed", exc_info=True)
            pass

    def get_node(self, node_id, cb=None):
        """
        Get node data from storage
        """
        self.get(prefix="node-", key=node_id, cb=cb)

    def delete_node(self, node_id, get_indexed_public, cb=None):
        """
        Delete node from storage
        """
        self.delete(prefix="node-", key=node_id, cb=None if get_indexed_public else cb)
        if get_indexed_public:
            self._delete_node_index(node_id, get_indexed_public, cb=cb)

    def _delete_node_index(self, node_id, indexes, cb=None):
        _log.analyze(node_id, "+", {'indexes': indexes})
        try:
            counter = [len(indexes)]  # counter value by reference used in callback
            for index in indexes:
                self.remove_index(index, node_id, cb=CalvinCB(self._delete_node_cb, counter=counter, org_cb=cb))
            # The remove index gets 1 second otherwise we call the callback anyway, i.e. stop the node
            async.DelayedCall(1.0, self._delete_node_timeout_cb, counter=counter, org_cb=cb)
        except:
            _log.debug("Remove node index failed", exc_info=True)
            if cb:
                cb()

    def _delete_node_cb(self, counter, org_cb, *args, **kwargs):
        _log.analyze(self.node.id, "+", {'counter': counter[0]})
        counter[0] = counter[0] - 1
        if counter[0] == 0:
            org_cb(*args, **kwargs)

    def _delete_node_timeout_cb(self, counter, org_cb):
        _log.analyze(self.node.id, "+", {'counter': counter[0]})
        if counter[0] > 0:
            _log.debug("Delete node index not finished but call callback anyway")
            org_cb()

    def add_application(self, application, cb=None):
        """
        Add application to storage
        """
        _log.debug("Add application %s id %s" % (application.name, application.id))

        self.set(prefix="application-", key=application.id,
                 value={"name": application.name,
                        "ns": application.ns,
                        # FIXME when all users of the actors field is updated, save the full dict only
                        "actors_name_map": application.actors,
                        "origin_node_id": application.origin_node_id,
                        "required_reliability":application.required_reliability},
                 cb=cb)

    def get_application(self, application_id, cb=None):
        """
        Get application from storage
        """
        self.get(prefix="application-", key=application_id, cb=cb)

    def get_application_actors(self, application_id, cb=None):
        """
        Get application from storage
        """
        return self.get_concat(prefix="app-actors-", key=application_id, cb=cb)

    def get_replica_nodes(self, app_id, actor_name, cb=None):
        """
        Get the nodes for where there exists replicas of the actor actor_name in the app app_id
        """
        return self.get_concat(prefix="replica-nodes-", key=app_id + ":" + calvinuuid.remove_uuid(actor_name), cb=cb)

    def get_node_actors(self, node_id, cb=None):
        """
        Get actors located on a specific node
        """
        return self.get_concat(prefix="node-actors-", key=node_id, cb=cb)

    def delete_application(self, application_id, cb=None):
        """
        Delete application from storage
        """
        _log.debug("Delete application %s" % application_id)
        self.delete(prefix="application-", key=application_id, cb=cb)

    def add_actor(self, actor, node_id, app_id, cb=None):
        """
        Add actor and its ports to storage
        """
        _log.debug("Add actor %s id %s" % (actor, node_id))

        if not actor.master_node:
            _log.debug("Setting master node of actor {} to {}".format(actor.name, node_id))
            actor.master_node = node_id

        data = {"name": actor.name, "type": actor._type, "master_node": actor.master_node, "node_id": node_id, 'app_id': app_id, 'replicate': actor.replicate}

        inports = []
        for p in actor.inports.values():
            port = {"id": p.id, "name": p.name}
            inports.append(port)
            self.add_port(p, node_id, actor.id, "in")
        data["inports"] = inports
        outports = []

        for p in actor.outports.values():
            port = {"id": p.id, "name": p.name}
            outports.append(port)
            self.add_port(p, node_id, actor.id, "out")
        data["outports"] = outports
        data["is_shadow"] = isinstance(actor, ShadowActor)

        if app_id:
            cb = CalvinCB(self.add_actor_to_app, app_id=app_id, actor_id=actor.id, cb=cb)
            name = calvinuuid.remove_uuid(actor.name)
            if node_id:
                cb = CalvinCB(self.add_replica_nodes, app_id=app_id, name=name, node_id=node_id, cb=cb)
                cb = CalvinCB(self.add_node_actor, node_id=node_id, actor_id=actor.id, cb=cb)
            else:
                _log.warning("Tried to add None to replica nodes of app {} actor {}".format(app_id, name))

        self.set(prefix="actor-", key=actor.id, value=data, cb=cb)
        self.trigger_flush()

    def add_actor_to_app(self, app_id, actor_id, cb=None, *args, **kwargs):
        _log.debug("Adding actor {} to app actors for app {}".format(actor_id, app_id))
        cb = CalvinCB(func=self.append_cb, org_key=None, org_value=None, org_cb=cb)
        self.append("app-actors-", key=app_id, value=[actor_id], cb=cb)

    def add_replica_nodes(self, app_id, name, node_id, cb=None, *args, **kwargs):
        _log.debug("Adding node {} to replica nodes of app {} actor name {}".format(node_id, app_id, name))
        cb = CalvinCB(func=self.append_cb, org_key=None, org_value=None, org_cb=cb)
        self.append("replica-nodes-", key=app_id + ":" + name, value=[node_id], cb=cb)

    def add_node_actor(self, node_id, actor_id, cb=None, *args, **kwargs):
        _log.debug("Adding actor {} to node actors for node {}".format(actor_id, node_id))
        cb = CalvinCB(func=self.append_cb, org_key=None, org_value=None, org_cb=cb)
        self.append("node-actors-", key=node_id, value=[actor_id], cb=cb)
        self.trigger_flush()

    def get_actor(self, actor_id, cb=None):
        """
        Get actor from storage
        """
        self.get(prefix="actor-", key=actor_id, cb=cb)

    def delete_actor(self, actor_id, cb=None):
        """
        Delete actor from storage
        """
        _log.debug("Delete actor id %s" % (actor_id))
        self.delete(prefix="actor-", key=actor_id, cb=cb)

    def delete_actor_from_node(self, node_id, actor_id):
        if not node_id:
            _log.warning("Cannot delete actor because node id is None")
            return

        _log.debug("Deleting actor {} from node {}".format(actor_id, node_id))
        self.remove("node-actors-", key=node_id, value=[actor_id], cb=None)

    def delete_actor_from_app(self, app_id, actor_id):
        """Remove actor_id from application's list of actors"""
        if not app_id:
            _log.warning("Cannot delete actor because app id is None")
            return

        self.remove("app-actors-", key=app_id, value=[actor_id], cb=None)

    def delete_replica_node(self, app_id, node_id, actor_name, cb=None):
        """
        Delete node_id from the list of replica nodes
        """
        name = calvinuuid.remove_uuid(actor_name)
        _log.debug("Deleting node {} from app {} replica nodes of actor {}".format(node_id, app_id, name))
        self.remove("replica-nodes-", key=app_id + ":" + name, value=[node_id], cb=cb)

    def new_replication_time(self, actor_type, replication_time, cb=None):
        _log.debug("Storing replication time {} of actor type {}".format(replication_time, actor_type))
        cb = CalvinCB(func=self.append_cb, org_key=None, org_value=None, org_cb=cb)
        self.append("replication-time-", key=actor_type, value=[replication_time], cb=cb)

    def get_replication_times(self, actor_type, cb=None):
        """
        Get replication times for actor_type
        """
        self.get_concat(prefix="replication-time-", key=actor_type, cb=cb)

    def add_failure_time(self, uri, node_id, timestamp, cb=None):
        """Store a time for failure for node with uri uri"""
        _log.debug("Checking if we should store failure time {} for uri {}".format(timestamp, uri))
        cb = CalvinCB(self._add_failure_time, node_id=node_id, timestamp=timestamp, cb=cb)
        self.get_concat(prefix="failure-node-ids-", key=uri, cb=cb)

    def _add_failure_time(self, key, value, node_id, timestamp, cb):
        if value and node_id in value:
            _log.warning("Already stored failure time for node {} - {}".format(key, node_id))
            timestamp = []
        else:
            _log.debug("Failure time for {} not stored yet, storing".format(node_id))
            timestamp = [timestamp]

        cb = CalvinCB(func=self._add_uri_failure_time, uri=key, timestamp=timestamp, cb=cb)
        cb = CalvinCB(func=self.append_cb, org_key=None, org_value=None, org_cb=cb)
        self.append("failure-node-ids-", key=key, value=[node_id], cb=cb)

    def _add_uri_failure_time(self, key, value, uri, timestamp, cb):
        cb = CalvinCB(func=self.append_cb, org_key=None, org_value=None, org_cb=cb)
        _log.debug("Storing failure time {} for uri {}".format(timestamp, uri))
        self.append("failure-times-", key=uri, value=timestamp, cb=cb)
        self.trigger_flush()

    def get_failure_times(self, uri, cb=None):
        """
        Get failure times for actor_type
        """
        self.get_concat(prefix="failure-times-", key=uri, cb=cb)

    def add_port(self, port, node_id, actor_id=None, direction=None, cb=None):
        """
        Add port to storage
        """
        if direction is None:
            if isinstance(port, actorport.InPort):
                direction = "in"
            else:
                direction = "out"

        if actor_id is None:
            actor_id = port.owner.id

        data = {"name": port.name, "connected": port.is_connected(
        ), "node_id": node_id, "actor_id": actor_id, "direction": direction}
        if direction == "out":
            if port.is_connected():
                data["peers"] = [(peer.node_id, peer.port_id) for peer in port.get_peers()]
            else:
                data["peers"] = []
        elif direction == "in":
            if port.is_connected():
                data["peers"] = [(peer.node_id, peer.port_id) for peer in port.get_peers()]
            else:
                data["peers"] = []
        self.set(prefix="port-", key=port.id, value=data, cb=cb)

    def get_port(self, port_id, cb=None):
        """
        Get port from storage
        """
        self.get(prefix="port-", key=port_id, cb=cb)

    def delete_port(self, port_id, cb=None):
        """
        Delete port from storage
        """
        self.delete(prefix="port-", key=port_id, cb=cb)

    def index_cb(self, key, value, org_cb, index_items):
        """
        Collect all the index levels operations into one callback
        """
        _log.debug("index cb key:%s, value:%s, index_items:%s" % (key, value, index_items))
        #org_key = key.partition("-")[2]
        org_key = key
        # cb False if not already done it at first False value
        if not value and index_items:
            org_cb(key=org_key, value=False)
            del index_items[:]
        if org_key in index_items:
            # remove this index level from list
            index_items.remove(org_key)
            # If all done send True
            if not index_items:
                org_cb(key=org_key, value=True)

    def _index_strings(self, index, root_prefix_level):
        # Make the list of index levels that should be used
        # The index string must been escaped with \/ and \\ for / and \ within levels, respectively
        if isinstance(index, list):
            items = index
        else:
            items = re.split(r'(?<![^\\]\\)/', index.lstrip("/"))
        root = "/".join(items[:root_prefix_level])
        del items[:root_prefix_level]
        items.insert(0, root)

        # index strings for all levels
        indexes = ['/'+'/'.join(items[:l]) for l in range(1,len(items)+1)]
        return indexes

    def add_index(self, index, value, root_prefix_level=3, cb=None):
        """
        Add value (typically a node id) to the storage as a set.
        index: a string with slash as delimiter for finer level of index,
               e.g. node/address/example_street/3/buildingA/level3/room3003,
               node/affiliation/owner/com.ericsson/Harald,
               node/affiliation/name/com.ericsson/laptop
               OR a list of strings
        value: the value that is to be added to the set stored at each level of the index
        root_prefix_level: the top level of the index that can be searched,
               with =1 then e.g. node/address, node/affiliation
        cb: will be called when done.
        """

        # TODO this implementation will store the value to each level of the index.
        # When time permits a proper implementation should be done with for example
        # a prefix hash table on top of the DHT or using other storage backend with
        # prefix search built in.

        _log.debug("add index %s: %s" % (index, value))

        indexes = self._index_strings(index, root_prefix_level)

        # make copy of indexes since altered in callbacks
        for i in indexes[:]:
            self.append(prefix="index-", key=i, value=[value],
                        cb=CalvinCB(self.index_cb, org_cb=cb, index_items=indexes) if cb else None)

    def remove_index(self, index, value, root_prefix_level=2, cb=None):
        """
        Remove value (typically a node id) from the storage as a set.
        index: a string with slash as delimiter for finer level of index,
               e.g. node/address/example_street/3/buildingA/level3/room3003,
               node/affiliation/owner/com.ericsson/Harald,
               node/affiliation/name/com.ericsson/laptop
        value: the value that is to be removed from the set stored at each level of the index
        root_prefix_level: the top level of the index that can be searched,
               with =1 then e.g. node/address, node/affiliation
        cb: will be called when done.
        """

        # TODO this implementation will delete the value to each level of the index.
        # When time permits a proper implementation should be done with for example
        # a prefix hash table on top of the DHT or using other storage backend with
        # prefix search built in.

        # TODO Currently we don't go deeper than the specified index for a remove,
        # e.g. node/affiliation/owner/com.ericsson would remove the value from
        # all deeper indeces. But no current use case exist either.

        _log.debug("remove index %s: %s" % (index, value))

        indexes = self._index_strings(index, root_prefix_level)

        # make copy of indexes since altered in callbacks
        for i in indexes[:]:
            self.remove(prefix="index-", key=i, value=[value],
                        cb=CalvinCB(self.index_cb, org_cb=cb, index_items=indexes) if cb else None)

    def get_index(self, index, cb=None):
        """
        Get index from the storage.
        index: a string with slash as delimiter for finer level of index,
               e.g. node/address/example_street/3/buildingA/level3/room3003,
               node/affiliation/owner/com.ericsson/Harald,
               node/affiliation/name/com.ericsson/laptop
        cb: will be called when done. Should expect to be called several times with
               partial results. Currently only called once.

        Since storage might be eventually consistent caller must expect that the
        list can containe node ids that are removed and node ids have not yet reached
        the storage.
        """

        # TODO this implementation will get the value from the level of the index.
        # When time permits a proper implementation should be done with for example
        # a prefix hash table on top of the DHT or using other storage backend with
        # prefix search built in. A proper implementation might also have several callbacks
        # since might get index from several levels of index trie, and instead of building a complete
        # list before returning better to return iteratively for nodes with less memory
        # or system with large number of nodes, might also need a timeout.

        if isinstance(index, list):
            index = "/".join(index)

        if not index.startswith("/"):
            index = "/" + index
        _log.debug("get index %s" % (index))
        self.get_concat(prefix="index-", key=index, cb=cb)

    def get_index_iter(self, index, include_key=False):
        """
        Get index from the storage.
        index: a string with slash as delimiter for finer level of index,
               e.g. node/address/example_street/3/buildingA/level3/room3003,
               node/affiliation/owner/com.ericsson/Harald,
               node/affiliation/name/com.ericsson/laptop

        Since storage might be eventually consistent caller must expect that the
        list can containe node ids that are removed and node ids have not yet reached
        the storage.
        """

        # TODO this implementation will get the value from the level of the index.
        # When time permits a proper implementation should be done with for example
        # a prefix hash table on top of the DHT or using other storage backend with
        # prefix search built in.

        if isinstance(index, list):
            index = "/".join(index)

        if not index.startswith("/"):
            index = "/" + index
        _log.debug("get index iter %s" % (index))
        return self.get_concat_iter(prefix="index-", key=index, include_key=include_key)

    ### Storage proxy server ###

    def tunnel_request_handles(self, tunnel):
        """ Incoming tunnel request for storage proxy server"""
        # TODO check if we want a tunnel first
        _log.analyze(self.node.id, "+ SERVER", {'tunnel_id': tunnel.id})
        self.tunnel[tunnel.peer_node_id] = tunnel
        tunnel.register_tunnel_down(CalvinCB(self.tunnel_down, tunnel))
        tunnel.register_tunnel_up(CalvinCB(self.tunnel_up, tunnel))
        tunnel.register_recv(CalvinCB(self.tunnel_recv_handler, tunnel))
        # We accept it by returning True
        return True

    def tunnel_down(self, tunnel):
        """ Callback that the tunnel is not accepted or is going down """
        _log.analyze(self.node.id, "+ SERVER", {'tunnel_id': tunnel.id})
        # We should always return True which sends an ACK on the destruction of the tunnel
        return True

    def tunnel_up(self, tunnel):
        """ Callback that the tunnel is working """
        _log.analyze(self.node.id, "+ SERVER", {'tunnel_id': tunnel.id})
        # We should always return True which sends an ACK on the destruction of the tunnel
        return True

    def _proxy_reply(self, cb, *args, **kwargs):
        # Should not get any replies to the server but log it just in case
        _log.analyze(self.node.id, "+ SERVER", {args: args, 'kwargs': kwargs})

    def tunnel_recv_handler(self, tunnel, payload):
        """ Gets called when a storage client request"""
        _log.debug("Storage proxy request %s" % payload)
        _log.analyze(self.node.id, "+ SERVER", {'payload': payload})
        if 'cmd' in payload and payload['cmd'] in self._proxy_cmds:
            if 'value' in payload:
                if payload['cmd'] == 'SET' and payload['value'] is None:
                    # We detected a delete operation, since a set op with unencoded None is a delete
                    payload['cmd'] = 'DELETE'
                    payload.pop('value')
                else:
                    # Normal set op, but it will be encoded again in the set func when external storage, hence decode
                    payload['value']=self.coder.decode(payload['value'])
            # Call this nodes storage methods, which could be local or DHT,
            # prefix is empty since that is already in the key (due to these calls come from the storage plugin level).
            # If we are doing a get or get_concat then the result needs to be encoded, to correspond with what the
            # client's higher level expect from storage plugin level.
            self._proxy_cmds[payload['cmd']](cb=CalvinCB(self._proxy_send_reply, tunnel=tunnel,
                                                        encode=True if payload['cmd'] in ('GET', 'GET_CONCAT') else False,
                                                        msgid=payload['msg_uuid']),
                                             prefix="",
                                             **{k: v for k, v in payload.iteritems() if k in ('key', 'value')})
        else:
            _log.error("Unknown storage proxy request %s" % payload['cmd'] if 'cmd' in payload else "")

    def _proxy_send_reply(self, key, value, tunnel, encode, msgid):
        _log.analyze(self.node.id, "+ SERVER", {'msgid': msgid, 'key': key, 'value': value})
        tunnel.send({'cmd': 'REPLY', 'msg_uuid': msgid, 'key': key, 'value': self.coder.encode(value) if encode else value})
