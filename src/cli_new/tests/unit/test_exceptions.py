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

# pylint: disable=redefined-outer-name,missing-docstring

import mock

from mesos.exceptions import (
    MesosHTTPException,
    MesosAuthenticationException,
    MesosAuthorizationException,
    MesosBadRequest,
    MesosUnprocessableException,
)


def test_exceptions():
    """
    Test exceptions
    """
    mock_resp = mock.Mock()
    mock_resp.status_code = 400
    mock_resp.reason = "some_reason"
    mock_resp.request.url = "http://some.url"
    mock_resp.text = "something bad happened"

    # Test MesosHTTPException
    err = MesosHTTPException(mock_resp)
    assert err.status() == 400
    assert str(err) == "Error while fetching [http://some.url]: " \
                       "HTTP 400: some_reason"

    # Test MesosAuthenticationException
    authn_err = MesosAuthenticationException(mock_resp)
    assert str(authn_err) == "Authentication failed."

    # Test MesosAuthorizationException
    authz_err = MesosAuthorizationException(mock_resp)
    assert str(authz_err) == "You are not authorized to perform this operation."

    # Test MesosBadRequest
    badreq_err = MesosBadRequest(mock_resp)
    assert str(badreq_err) == "Bad request."

    # Test MesosUnprocessableException
    unproc_err = MesosUnprocessableException(mock_resp)
    assert str(unproc_err) == "Error while fetching [http://some.url]: " \
                              "HTTP 400: something bad happened"
