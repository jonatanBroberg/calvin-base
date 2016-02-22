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
import time
from calvin.actor.actor import Actor, ActionResult, manage, condition

class DyingPrint(Actor):
    """
    Counts and prints the first ten inputs and then throws an exception 

    Input:
      token : Any token
    """

    def exception_handler(self, action, args, context):
        # Check args to verify that it is EOSToken
        return action(self, *args)

    @manage(exclude = ['start_time'])
    def init(self, lifetime = 30):
        self.start_time = self._millis()
        self.lifetime = 1000 * lifetime

    @condition(action_input=['token'])
    def action(self, token):
        try:
            current = self.start_time + self.lifetime
        except:
            self.start_time = self._millis()
            current = self.start_time
        print str(token).strip()
        if self.start_time + self.lifetime < self._millis():
            os._exit(0)
        return ActionResult()

    def _millis(self):
        return int(round(time.time() * 1000))

    action_priority = (action, )

