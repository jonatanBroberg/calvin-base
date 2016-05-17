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

import unittest
import json

from mock import Mock

from calvin.runtime.north.actormanager import ActorManager
from calvin.runtime.north import metering
from calvin.runtime.south.endpoint import LocalInEndpoint
import calvin.utilities.calvinresponse as response


class DummyPortManager:

    def disconnect(self, callback=None, actor_id=None, port_name=None, port_dir=None, port_id=None):
        status = response.CalvinResponse(True)
        if callback:
            callback(status=status, actor_id=actor_id, port_name=port_name, port_id=port_id)

    def add_ports_of_actor(self, actor):
        pass

    def remove_ports_of_actor(self, actor):
        pass


class StorageMock(object):
    def __init__(self, *args, **kwargs):
        self.started = True
    def add_actor(self, actor, node_id, app_id, cb=None):
        if cb:
            cb(key=actor.id, value=actor)
    def delete_actor(self, actor_id, cb=None):
        if cb:
            cb(actor_id, True)
    def delete_actor_from_node(self, node_id, actor_id):
        pass


class ConnectionHandler(object):
    def setup_replica_connections(self, actor, state, prev_connections, callback):
        for name, port in actor.inports.iteritems():
            port.endpoints.append(LocalInEndpoint(port, port))
        for name, port in actor.outports.iteritems():
            port.endpoints.append(LocalInEndpoint(port, port))

        if callback:
            callback(status=response.CalvinResponse(True))

    def setup_connections(self, actor, prev_connections, connection_list, callback):
        if callback:
            callback(status=response.CalvinResponse(True))


class DummyNode:

    def __init__(self):
        self.id = id(self)
        self.pm = DummyPortManager()
        self.storage = StorageMock()
        self.control = Mock()
        self.metering = metering.set_metering(metering.Metering(self))
        self.app_manager = Mock()

    def calvinsys(self):
        return None


class ActorManagerTests(unittest.TestCase):

    def setUp(self):
        n = DummyNode()
        self.am = ActorManager(node=n)
        self.am.connection_handler = ConnectionHandler()
        n.am = self.am

    def tearDown(self):
        pass

    def _new_actor(self, a_type, a_args, app_id=None, **kwargs):
        a_id = self.am.new(a_type, a_args, app_id=app_id, **kwargs)
        a = self.am.actors.get(a_id, None)
        self.assertTrue(a)
        return a, a_id

    def testNewActor(self):
        # Test basic actor creation
        a_type = 'std.Constant'
        data = 42
        a, _ = self._new_actor(a_type, {'data':data})
        self.assertEqual(a.data, data)

    def testActorStateGet(self):
        # Test basic actor state retrieval
        a_type = 'std.Constant'
        data = 42
        a, a_id = self._new_actor(a_type, {'data':data})
        s = a.state()

        self.assertEqual(s['data'], data)
        self.assertEqual(s['id'], a_id)
        self.assertEqual(s['n'], 1)

    def testNewActorFromState(self):
        # Test basic actor state manipulation
        a_type = 'std.Constant'
        data = 42
        a, a_id = self._new_actor(a_type, {'data': data})
        a.data = 43
        a.n = 2
        s = a.state()
        self.am.delete_actor(a_id)
        self.assertEqual(len(self.am.actors), 0)

        b, b_id = self._new_actor(a_type, None, state=s)

        self.assertEqual(a.data, 43)
        self.assertEqual(a.n, 2)
        # Assert id is preserved
        self.assertEqual(a.id, a_id)
        # Assert actor database is consistent
        self.assertTrue(self.am.actors[a_id])
        self.assertEqual(len(self.am.actors), 1)

    def testNewReplicaWithState(self):
        a_type = 'io.StandardOut'
        a, a_id = self._new_actor(a_type, {'name': 'a_name', 'store_tokens': 1})
        a.tokens = [1, 2, 3]
        state = a.state()

        prev_connections = a.connections(self.am.node.id)
        prev_connections['port_names'] = a.port_names()

        args = a.replication_args()
        args['name'] = 'new_name'
        self.am.new_replica(a_type, args, state, dict(prev_connections), None, None)
        self.assertEqual(len(self.am.actors), 2)

        ids = set(self.am.actors.keys())
        b_id = (ids | set([a.id])).pop()
        b = self.am.actors[b_id]

        self.assertEqual(a.tokens, [1, 2, 3])
        self.assertEqual(a.store_tokens, 1)
        self.assertEqual(b.tokens, [1, 2, 3])
        self.assertEqual(b.store_tokens, 1)

        # Assert new id is assigned
        self.assertNotEqual(b.id, a.id)

        # Assert new name is assigned
        self.assertNotEqual(b.name, a.name)

        # Assert actor database is consistent
        self.assertTrue(self.am.actors[a_id])
        self.assertTrue(self.am.actors[b.id])
        self.assertEqual(len(self.am.actors), 2)


if __name__ == '__main__':
    import unittest
    suite = unittest.TestLoader().loadTestsFromTestCase(ActorManagerTests)
    unittest.TextTestRunner(verbosity=2).run(suite)
