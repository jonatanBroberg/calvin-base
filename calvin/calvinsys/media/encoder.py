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

from calvin.runtime.south.plugins.media import encoder


class Encoder(object):

    """
    Encoder object
    """

    def __init__(self):
        """
        Initialize
        """
        self.encoder = encoder.Encoder()

    def encode(self, frame, encoding):
        """
        Encode frame
        """
        return self.encoder.encode(frame, encoding)


def register(node=None, actor=None):
    """
        Called when the system object is first created.
    """
    return Encoder()
