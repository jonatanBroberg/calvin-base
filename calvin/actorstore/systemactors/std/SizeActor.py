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

import os
import sys

from calvin.actor.actor import Actor, ActionResult, manage, condition


class SizeActor(Actor):
    """
    forward a token unchanged
    Inputs:
      token : a token
    Outputs:
      token : the same token
    """
    @manage(['dump', 'padding'])
    def init(self, size, dump=False):
        self.dump = dump
        self.padding = " " * size

    def log(self, data):
        print "%s<%s>: %s" % (self.__class__.__name__, self.id, data)

    @condition(['token'], ['token'])
    def donothing(self, input):
        if self.dump:
            self.log(input)
        return ActionResult(production=(input, ))

    action_priority = (donothing, )

    test_set = [
        {
            'in': {'token': [1, 2, 3]},
            'out': {'token': [1, 2, 3]}
        }
    ]
