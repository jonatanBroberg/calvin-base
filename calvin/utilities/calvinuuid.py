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

import calvinlogger
import logging
import uuid as sys_uuid
import re

uuid_re = "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"


def uuid(prefix):
    u = str(sys_uuid.uuid4())
    if calvinlogger.get_logger(__name__).getEffectiveLevel() == logging.DEBUG:
        return prefix + "_" + u
    else:
        return u


def remove_uuid(string):
    return re.sub(uuid_re, "", string)
