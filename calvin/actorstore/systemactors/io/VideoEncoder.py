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
import pickle
import socket
import time
import json
import copy
import numpy
import base64

from calvin.actor.actor import Actor, ActionResult, manage, condition, guard
from calvin.runtime.north.calvin_token import EOSToken, ExceptionToken


def json_numpy_obj_hook(dct, frame_buffer):
    """Decodes a previously encoded numpy ndarray with proper shape and dtype.

    :param dct: (dict) json encoded ndarray
    :return: (ndarray) if input was an encoded ndarray
    """
    if isinstance(dct, dict):
        data = base64.b64decode(frame_buffer)  # dct['ndarray'])
        #data = frame_buffer
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

    @condition(['in'])
    def encode(self, data):
        #print "ENCODER", data['frame_count'], time.time()
        #t0 = time.time()
        #print "yeah"
        data = copy.deepcopy(data)
        #print data
        url = data['url']
        host = url.split(":")[0]
        port = int(url.split(":")[1])

        frame = data.get('frame')

        frame_count = data['frame_count']
        if frame_count not in self.frame_buffer:
            self.frame_buffer[frame_count] = ""
        if frame is not None:
            self.frame_buffer[frame_count] = self.frame_buffer[frame_count] + frame['ndarray']
            if len(self.frame_buffer[frame_count]) != data['expected_length']:
                #print "returning...", time.time()
                return ActionResult(production=())
            else:
                #frame = pickle.loads(frame)
                #frame = numpy.array(frame)
                frame = json_numpy_obj_hook(frame, self.frame_buffer[frame_count])
                ret, jpeg = cv2.imencode(".jpg", frame)
                data['frame'] = jpeg.tobytes().decode("latin-1")
                self.frame_buffer[frame_count] = ""
        else:
            data['frame_count'] = -1
        #t00 = time.time()
        #print "INIT: ", t00 - t0

        #print time.time()
        #t1 = time.time()
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #t2 = time.time()
        try:
            s.connect((host, port))
            #t3 = time.time()
        except:
            #print "DONE?!?!?!"
            # we're done
            return ActionResult(production=())

        #print time.time()
        data['id'] = self.id

        try:
            #t4 = time.time()
            s.sendall(json.dumps(data))
            #t5 = time.time()
            #print "send", t5 - t4
        except Exception as e:
            #print "failed to send..", e
            pass
        #t6 = time.time()
        s.close()

        #print "socket ", t2 - t1
        #print "connect ", t3 - t2
        #print time.time()
        #print "close", t6 - t5
        #print time.time()

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
