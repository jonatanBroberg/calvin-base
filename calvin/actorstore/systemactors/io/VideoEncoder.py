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

import socket
import json
import copy

from calvin.actor.actor import Actor, ActionResult, manage, condition


class VideoEncoder(Actor):

    """
    Read a file line by line, and send each line as a token on output port

    Inputs:
      in : data to encode and send
    """

    @manage([])
    def init(self, replicate=False):
        self.did_read = False
        self.frame_buffer = {}
        self.done_sending = False
        self.use("calvinsys.media.encoder", shorthand="encoder")
        self.encoder = self["encoder"]
        self._replicate = replicate

    @property
    def replicate(self):
        return self._replicate

    @condition(['in'])
    def encode(self, data):
        data = copy.deepcopy(data)
        url = data['url']
        host = url.split(":")[0]
        port = int(url.split(":")[1])

        frame = data.get('frame')

        if isinstance(self.encoder, dict):
            self.use("calvinsys.media.encoder", shorthand="encoder")
            self.encoder = self["encoder"]

        if frame:
            data['frame'] = self.encoder.encode(frame, ".jpg").decode("latin-1")

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((host, port))
        except:
            return ActionResult(production=())

        data['id'] = self.id
        try:
            s.sendall(json.dumps(data))
        except:
            pass

        return ActionResult(production=())

    action_priority = (encode, )
    requires =  ['calvinsys.media.encoder']

    test_set = []
