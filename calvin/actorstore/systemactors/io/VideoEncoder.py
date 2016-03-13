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

import cv2
import socket
import json
import copy
import numpy
import base64

from calvin.actor.actor import Actor, ActionResult, manage, condition, guard


def json_numpy_obj_hook(dct):
    """Decodes a previously encoded numpy ndarray with proper shape and dtype.

    :param dct: (dict) json encoded ndarray
    :return: (ndarray) if input was an encoded ndarray
    """
    if isinstance(dct, dict) and 'ndarray' in dct:
        data = base64.b64decode(dct['ndarray'])
        return numpy.frombuffer(data, dct['dtype']).reshape(dct['shape'])
    return dct


def absolute_filename(filename):
    """Test helper - get absolute name of file
    @TODO: Possibly not the best way of doing this
    """
    import os.path
    return os.path.join(os.path.dirname(__file__), filename)


class VideoEncoder(Actor):

    """
    Read a file line by line, and send each line as a token on output port

    Inputs:
      in : data to encode and send
    """

    @manage([])
    def init(self):
        self.did_read = False
        self.use(requirement='calvinsys.io.filehandler', shorthand='file')
        self.frame_buffer = {}
        self.done_sending = False

    @condition(['in'])
    def encode(self, data):
        data = copy.deepcopy(data)
        url = data['url']
        host = url.split(":")[0]
        port = int(url.split(":")[1])

        frame = data.get('frame')

        if frame is not None:
            frame = json_numpy_obj_hook(frame)
            ret, jpeg = cv2.imencode(".jpg", frame)
            data['frame'] = jpeg.tobytes().decode("latin-1")

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
    requires =  ['calvinsys.io.filehandler']

    # Assumes file contains "A\nB\nC\nD\nE\nF\nG\nH\nI"

    test_set = [
        {  # Test 3, read a non-existing file
            'in': {'filename': "no such file"},
            'out': {'out': ["File not found"]}
        },
        {  # Test 1, read a file
            'in': {'filename': absolute_filename('data.txt')},
            'out': {'out': ['A', 'B', 'C', 'D']}
        },
        {  # Test 2, read more of file
            'out': {'out': ['E', 'F', 'G', 'H']}
        },
        {  # Test 3, read last of file
            'out': {'out': ['I']}
        }

    ]
