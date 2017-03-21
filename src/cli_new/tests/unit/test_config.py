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

from __future__ import absolute_import

import pytest

from mesos import config


def _conf():
    return {
        'mesos': {
            'user': 'principal',
            'mesos_uri': 'zk://localhost/mesos'
        },
        'package': {
            'repo_uri': 'git://localhost/mesosphere/package-repo.git'
        }
    }


@pytest.fixture
def conf():
    return config.Toml(_conf())


def test_get_property(conf):
    assert conf['mesos.mesos_uri'] == 'zk://localhost/mesos'


def test_get_partial_property(conf):
    assert list(conf['mesos'].property_items()) == list(config.Toml({
        'user': 'principal',
        'mesos_uri': 'zk://localhost/mesos'
    }).property_items())


def test_iterator(conf):
    assert (sorted(list(conf.property_items())) == [
        ('mesos.mesos_uri', 'zk://localhost/mesos'),
        ('mesos.user', 'principal'),
        ('package.repo_uri', 'git://localhost/mesosphere/package-repo.git'),
    ])


def test_len(conf):
    assert len(conf) == 2


@pytest.fixture
def mutable_conf():
    return config.MutableToml(_conf())


def test_mutable_unset_property(mutable_conf):
    expect = config.MutableToml({
        'mesos': {
            'user': 'principal',
            'mesos_uri': 'zk://localhost/mesos'
        },
        'package': {}
    })

    del mutable_conf['package.repo_uri']

    assert mutable_conf == expect


def test_mutable_set_property(mutable_conf):
    expect = config.MutableToml({
        'mesos': {
            'user': 'group',
            'mesos_uri': 'zk://localhost/mesos'
        },
        'package': {
            'repo_uri': 'git://localhost/mesosphere/package-repo.git'
        }
    })

    mutable_conf['mesos.user'] = 'group'

    assert mutable_conf == expect


def test_mutable_test_deep_property(mutable_conf):
    expect = config.MutableToml({
        'mesos': {
            'user': 'principal',
            'mesos_uri': 'zk://localhost/mesos'
        },
        'package': {
            'repo_uri': 'git://localhost/mesosphere/package-repo.git'
        },
        'new': {
            'key': 42
        },
    })

    mutable_conf['new.key'] = 42

    assert mutable_conf == expect


def test_mutable_get_property(mutable_conf):
    assert mutable_conf['mesos.mesos_uri'] == 'zk://localhost/mesos'
    assert list(mutable_conf['mesos'].property_items()) == \
           list(config.MutableToml({
               'user': 'principal',
               'mesos_uri': 'zk://localhost/mesos'
           }).property_items())


def test_mutable_iterator(mutable_conf):
    assert (sorted(list(mutable_conf.property_items())) == [
        ('mesos.mesos_uri', 'zk://localhost/mesos'),
        ('mesos.user', 'principal'),
        ('package.repo_uri', 'git://localhost/mesosphere/package-repo.git'),
    ])


def test_mutable_len(mutable_conf):
    assert len(mutable_conf) == 2
