# -*- coding: utf-8 -*-

# Copyright 2014-2015 Parsely, Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#     http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
This module exists solely for version information so we only have to change it
in one place. Based on the suggestion `here. <http://bit.ly/16LbuJF>`_

:organization: Parsely
"""


def _safe_int(string):
    """ Simple function to convert strings into ints without dying. """
    try:
        return int(string)
    except ValueError:
        return string


__version__ = "3.1.4.post1"
VERSION = tuple(_safe_int(x) for x in __version__.split("."))
