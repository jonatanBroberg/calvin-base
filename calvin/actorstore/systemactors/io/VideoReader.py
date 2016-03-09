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
import time
import base64
import time
import json
import pickle

from calvin.actor.actor import Actor, ActionResult, manage, condition, guard
from calvin.runtime.north.calvin_token import EOSToken, ExceptionToken


def absolute_filename(filename):
    """Test helper - get absolute name of file
    @TODO: Possibly not the best way of doing this
    """
    import os.path
    return os.path.join(os.path.dirname(__file__), filename)


class VideoReader(Actor):

    """
    Read a file line by line, and send each line as a token on output port

    Inputs:
      filename : File to read. If file doesn't exist, an ExceptionToken is produced
      send : #
    Outputs:
        out : data with frame, frame count and url to send it to
    """

    @manage([])
    def init(self):
        self.did_read = False
        self.file_not_found = False
        self.video = None
        self.use(requirement='calvinsys.io.filehandler', shorthand='file')
        self.end_of_file = False
        self.filename = None
        self.handle = None
        self.url = None
        self.active_frame = None
        self.active_frame_index = 0
        self.total_sent = 0
        self.active_frame_length = 0
        self.active_frame_data = ""

    @condition(['filename'])
    @guard(lambda self, filename: not self.video and not self.filename)
    def open_file(self, data):
        self.frame_count = 0
        data = json.loads(data)
        self.url = data['url']
        filename = data['filename']
        self.filename = filename
        try:
            self.video = cv2.VideoCapture("videos/" + filename)
            self.end_of_file = False
        except:
            self.video = None
            self.file_not_found = True
        return ActionResult()

    @condition([])
    @guard(lambda self: self.file_not_found)
    def file_not_found(self):
        token = ExceptionToken(value="File not found")
        self.file_not_found = False  # Only report once
        return ActionResult(production=(token, ))

    @condition(['send'])
    @guard(lambda self, send: self.video and self.url and not self.end_of_file and self.active_frame is None)
    def read(self, send):
        #print "\n"
        #print "READ", time.time()
        #print self.active_frame
        #print time.time()
        success, image = self.video.read()
        data = {
            'frame_count': -1,
            'url': self.url
        }
        if success:
            #data['frame'] = image.tolist()  # [0][0:10]  # .tolist()  # dumps()  # .decode("latin-1")
            height = len(image)
            length = len(image[0])
            #image = image[height / 4 : height - (height / 4), length / 4 : length - (length / 4)]
            #image = image[100:150, 200:300]  # height / 4 : height - (height / 4), length / 4 : length - (length / 4)]
            #data['frame'] = pickle.dumps(image, protocol=0)

            self.active_frame = image
            data_b64 = base64.b64encode(image.data)
            self.active_frame_data = data_b64
            self.active_frame_length = len(data_b64)
            data['frame'] = {
                "ndarray": data_b64,
                "dtype": str(image.dtype),
                "shape": image.shape
            }
            data['frame_count'] = self.frame_count
            #self.frame_count += 1
        else:
            self.end_of_file = True

        #time.sleep(2)  # 0.15)
        return ActionResult(production=(), did_fire=False)  # data, ))

    @condition([], ['out'])
    @guard(lambda self: self.active_frame is not None)
    def send(self):
        #print "SEND", self.frame_count, time.time()
        data = {
            'frame': {
                "dtype": str(self.active_frame.dtype),
                "shape": self.active_frame.shape
            },
            'url': self.url,
            'frame_count': self.frame_count,
            'expected_length': self.active_frame_length,
        }
        size = 30 * 1024
        if self.active_frame_index + size < self.active_frame_length - 1:
            data_b64 = self.active_frame_data[self.active_frame_index : self.active_frame_index + size]
            data['frame']['ndarray'] = data_b64
            self.active_frame_index = self.active_frame_index + size
            self.total_sent += len(data_b64)
        else:
            data_b64 = self.active_frame_data[self.active_frame_index : ]
            self.total_sent += len(data_b64)
            self.total_sent = 0
            self.active_frame_data = ""
            data['frame']["ndarray"] = data_b64
            #data['frame_count'] = self.frame_count
            self.frame_count += 1
            self.active_frame_index = 0
            self.active_frame = None

        #print "sending:", len(data['frame']['ndarray'])
        #print time.time()
        return ActionResult(production=(data,))  # data, ))

    @condition([])
    @guard(lambda self: self.video and self.end_of_file)
    def eof(self):
        self.video.release()
        self.video = None
        self.filename = None
        return ActionResult()  # production=(b"", ))  # EOSToken(), ))

    action_priority = (open_file, file_not_found, send, read, eof)
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
