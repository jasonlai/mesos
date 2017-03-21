# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Module for Serve Sent Events
"""

from __future__ import absolute_import

from sseclient import SSEClient

from mesos import http


def get(url, **kwargs):
    """ Make a get request to streaming endpoint which
    implements SSE (Server sent events). The parameter session=http
    will ensure we are using `dcos.http` module with all required auth
    headers.

    :param url: server sent events streaming URL
    :type url: str
    :param kwargs: arbitrary params for requests
    :type kwargs: dict
    :return: instance of sseclient.SSEClient
    :rtype: sseclient.SSEClient
    """
    return SSEClient(url, session=http, **kwargs)
