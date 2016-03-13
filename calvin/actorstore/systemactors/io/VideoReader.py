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
      trigger : #
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
        self.url = None
        self.can_read = False

    @condition(['filename'])
    @guard(lambda self, filename: not self.video and not self.filename)
    def open_file(self, data):
        self.frame_count = 0
        data = json.loads(data)
        self.url = data['url']
        filename = data['filename']
        self.filename = filename
        self.can_read = True
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

    @condition([], ['out'])
    @guard(lambda self: self.video and self.url and not self.end_of_file and self.can_read)
    def read(self):
        success, image = self.video.read()
        data = {
            'frame_count': -1,
            'url': self.url
        }
        if success:
            data_b64 = base64.b64encode(image.data)
            data['frame'] = {
                "ndarray": data_b64,
                "dtype": str(image.dtype),
                "shape": image.shape
            }
            data['frame_count'] = self.frame_count
            self.frame_count += 1
        else:
            self.end_of_file = True

        self.can_read = False
        return ActionResult(production=(data, ))

    @condition(['trigger'])
    @guard(lambda self, token: self.video and not self.end_of_file)
    def trigger(self, token):
        self.can_read = True
        return ActionResult(production=())

    @condition([])
    @guard(lambda self: self.video and self.end_of_file)
    def eof(self):
        self.video.release()
        self.video = None
        self.filename = None
        self.can_read = False
        return ActionResult()  # production=(b"", ))  # EOSToken(), ))

    action_priority = (open_file, file_not_found, read, trigger, eof)
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
