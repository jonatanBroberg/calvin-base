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
from calvin.runtime.north import fifo
from calvin.runtime.north.calvin_token import Token


class FifoTests(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def verify_data(self, write_data, fifo_data):
        print write_data, fifo_data
        for a, b in zip(write_data, fifo_data):
            d = b.value
            self.assertEquals(a, d)

    def test1(self):
        """Adding reader again (reconnect)"""
        f = fifo.FIFO(5)
        r1 = "r1"
        f.add_reader(r1)
        data = ['1', '2', '3', '4']
        for token in data:
            self.assertTrue(f.can_write(r1))
            self.assertTrue(f.write(Token(token), r1))

        self.verify_data(['1', '2'], [f.read(r1) for _ in range(2)])

        f.commit_reads(r1, True)
        f.add_reader(r1)

        self.verify_data(['3', '4'], [f.read(r1) for _ in range(2)])

        self.assertEquals(None, f.read(r1))

        f.commit_reads(r1, True)

        for token in ['5', '6', '7', '8']:
            self.assertTrue(f.can_write(r1))
            self.assertTrue(f.write(Token(token), r1))

        self.assertFalse(f.can_write(r1))

        self.verify_data(['5', '6', '7', '8'], [f.read(r1)
                                                for _ in range(4)])
        f.commit_reads(r1, True)

    def test2(self):
        """Multiple readers"""

        f = fifo.FIFO(5)
        r1 = "r1"
        r2 = "r2"
        f.add_reader(r1)
        f.add_reader(r2)

        # Ensure fifo is empty
        self.assertEquals(f.read(r1), None)
        self.assertEquals(f.length(r1), 0)

        # Add something
        self.assertTrue(f.write(Token('1'), r1))
        self.assertEquals(f.length(r1), 1)

        self.assertTrue(f.write(Token('1'), r2))
        self.assertEquals(f.length(r2), 1)

        # Reader r1 read something
        self.assertTrue(f.read(r1))
        f.commit_reads(r1)

        self.assertEquals([True] * 3, [f.write(Token(t), r1) for t in ['2', '3', '4']])
        self.assertEquals([True] * 3, [f.write(Token(t), r2) for t in ['2', '3', '4']])

        self.verify_data(['2', '3', '4'], [f.read(r1) for _ in range(3)])
        f.commit_reads(r1)

        # Reader r1 all done, ensure reader r2 can still read
        self.assertEquals(f.length(r1), 0)
        self.assertEquals(f.length(r2), 4)

        self.assertTrue(f.write(Token('5'), r1))
        self.assertFalse(f.write(Token('5'), r2))

        self.assertTrue(f.can_write(r1))
        self.assertFalse(f.can_write(r2))

        self.assertTrue(f.can_read(r2))
        self.assertTrue(f.can_read(r1))

        # Reader r2 reads something
        self.verify_data(['1', '2', '3'], [f.read(r2) for _ in range(3)])
        f.commit_reads(r2)
        self.verify_data(['4'], [f.read(r2) for _ in range(1)])

        self.assertEquals(f.length(r1), 1)
        self.assertTrue(f.can_read(r1))
        self.verify_data(['5'], [f.read(r1)])

        self.assertFalse(f.can_read(r2))
        self.assertEquals(None, f.read(r2))

        f.commit_reads(r2)
        f.commit_reads(r1)

        self.assertTrue(f.write(Token('6'), r1))
        self.assertTrue(f.write(Token('7'), r1))
        self.assertTrue(f.write(Token('8'), r1))

        self.assertEquals(f.length(r1), 3)

    def test3(self):
        """Testing commit reads"""
        f = fifo.FIFO(5)
        r1 = "r1"
        f.add_reader(r1)

        for token in ['1', '2', '3', '4']:
            self.assertTrue(f.can_write(r1))
            self.assertTrue(f.write(Token(token), r1))

        # Fails, fifo full
        self.assertFalse(f.can_write(r1))
        self.assertFalse(f.write(Token('5'), r1))

        # Tentative, fifo still full
        self.verify_data(['1'], [f.read(r1)])
        self.assertFalse(f.can_write(r1))
        self.assertFalse(f.write(Token('5'), r1))

        # commit previous reads, fifo 1 pos free
        f.commit_reads(r1)
        self.assertTrue(f.can_write(r1))
        self.assertTrue(f.write(Token('5'), r1))
        # fifo full again
        self.assertFalse(f.can_write(r1))
        self.assertFalse(f.write(Token('5'), r1))

    def test4(self):
        """Testing rollback reads"""
        f = fifo.FIFO(5)
        r1 = "r1"
        f.add_reader(r1)

        for token in ['1', '2', '3', '4']:
            self.assertTrue(f.can_write(r1))
            self.assertTrue(f.write(Token(token), r1))

        # fifo full
        self.assertFalse(f.can_write(r1))
        self.assertFalse(f.write(Token('5'), r1))

        # tentative reads
        self.verify_data(['1', '2', '3', '4'], [f.read(r1)
                                                for _ in range(4)])
        # len unchanged
        self.assertEquals(f.length(r1), 4)

        f.rollback_reads(r1)
        self.assertFalse(f.can_write(r1))
        self.assertFalse(f.write(Token('5'), r1))
        self.assertEquals(f.length(r1), 4)

        # re-read
        self.verify_data(['1'], [f.read(r1)])
        f.commit_reads(r1)
        self.assertEquals(f.length(r1), 3)

        # one pos free in fifo
        self.assertTrue(f.can_write(r1))
        self.assertTrue(f.write(Token('a'), r1))
        self.assertFalse(f.can_write(r1))
        self.assertFalse(f.write(Token('b'), r1))

    def test_fifo5(self):
        f = fifo.FIFO(5)
        r = "r"
        f.add_reader(r)

        for token in ['1', '2', '3', '4']:
            self.assertTrue(f.can_write(r))
            self.assertTrue(f.write(Token(token), r))

        self.assertFalse(f.can_write(r))
        self.assertTrue(f.can_read(r))
        self.verify_data(['1', '2', '3', '4'], [f.read(r) for _ in range(4)])
        self.assertFalse(f.can_write(r))
        f.commit_reads(r)
        self.assertTrue(f.can_write(r))
        self.assertFalse(f.can_read(r))
