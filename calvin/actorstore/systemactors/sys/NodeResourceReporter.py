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

from calvin.actor.actor import Actor, ActionResult, manage, condition, guard
from calvin.utilities.calvinlogger import get_logger

_log = get_logger(__name__)


class NodeResourceReporter(Actor):
    """
    Sends node's CPU usage to output. Is supposed to be connected to itself.

    Outputs:
      out : A JSON string containing System CPU Usage
    Inputs:
      in : A JSON string containing System CPU Usage
    """

    @manage([])
    def init(self, node, delay=1, quiet=True):
        self.node = node
        self.delay = delay
        self.quiet = quiet
        self._setup()

    def _setup(self):
        self.use('calvinsys.native.python-psutil', shorthand='psutil')
        self.use('calvinsys.events.timer', shorthand='timer')
        self.timer = self['timer'].repeat(self.delay)
        if self.quiet:
            self.logger = _log.debug
        else:
            self.logger = _log.info

    @condition(['in'])
    def tokenAvailable(self, usage):
        self.node.report_resource_usage(usage)
        return ActionResult()

    @condition([], ['out'])
    @guard(lambda self: self.timer.triggered)
    def timeout(self):
        self.timer.ack()
        usage = {'cpu_percent': self['psutil'].cpu_percent()}
        self.logger("%s<%s>: %s" % (self.__class__.__name__, self.node.id, str(usage)))
        return ActionResult(production=(usage, ))

    action_priority = (timeout, tokenAvailable)
    requires = ['calvinsys.native.python-psutil', 'calvinsys.events.timer']
