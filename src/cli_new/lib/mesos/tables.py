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
Various table formatters
"""

from __future__ import absolute_import

import datetime
import posixpath

from collections import OrderedDict

import prettytable

from mesos import util

EMPTY_ENTRY = '---'

DEPLOYMENT_DISPLAY = {'ResolveArtifacts': 'artifacts',
                      'ScaleApplication': 'scale',
                      'StartApplication': 'start',
                      'StopApplication': 'stop',
                      'RestartApplication': 'restart',
                      'ScalePod': 'scale',
                      'StartPod': 'start',
                      'StopPod': 'stop',
                      'RestartPod': 'restart',
                      'KillAllOldTasksOf': 'kill-tasks'}

LOGGER = util.get_logger(__name__)


def task_table(tasks):
    """Returns a PrettyTable representation of the provided mesos tasks.

    :param tasks: tasks to render
    :type tasks: [Task]
    :rtype: PrettyTable
    """

    fields = OrderedDict([
        ("NAME", lambda t: t["name"]),
        ("HOST", lambda t: t.slave()["hostname"]),
        ("USER", lambda t: t.user()),
        ("STATE", lambda t: t["state"].split("_")[-1][0]),
        ("ID", lambda t: t["id"]),
    ])

    tbl = table(fields, tasks, sortby="NAME")
    tbl.align["NAME"] = "l"
    tbl.align["HOST"] = "l"
    tbl.align["ID"] = "l"

    return tbl


def _format_unix_timestamp(tmstmp):
    """ Formats a unix timestamp in a `dcos task ls --long` format.

    :param tmstmp: unix timestamp
    :type tmstmp: int
    :rtype: str
    """
    return datetime.datetime.fromtimestamp(tmstmp).strftime('%b %d %H:%M')


def ls_long_table(files):
    """Returns a PrettyTable representation of `files`

    :param files: Files to render.  Of the form returned from the
        mesos /files/browse.json endpoint.
    :param files: [dict]
    :rtype: PrettyTable
    """

    fields = OrderedDict([
        ('MODE', lambda f: f['mode']),
        ('NLINK', lambda f: f['nlink']),
        ('UID', lambda f: f['uid']),
        ('GID', lambda f: f['gid']),
        ('SIZE', lambda f: f['size']),
        ('DATE', lambda f: _format_unix_timestamp(int(f['mtime']))),
        ('PATH', lambda f: posixpath.basename(f['path']))])

    tbl = table(fields, files, sortby="PATH", header=False)
    tbl.align = 'r'
    return tbl


def truncate_table(fields, objs, **kwargs):
    """Returns a PrettyTable.  `fields` represents the header schema of
    the table.  `objs` represents the objects to be rendered into
    rows.

    :param fields: An OrderedDict, where each element represents a
                   column.  The key is the column header, and the
                   value is the function that transforms an element of
                   `objs` into a value for that column.
    :type fields: OrderdDict(str, function)
    :param objs: objects to render into rows
    :type objs: [object]
    :param limits: limits for truncating for each row
    :type limits: [object]
    :param **kwargs: kwargs to pass to `prettytable.PrettyTable`
    :type **kwargs: dict
    :rtype: PrettyTable
    """

    tbl = prettytable.PrettyTable(
        [k.upper() for k in fields.keys()],
        border=False,
        hrules=prettytable.NONE,
        vrules=prettytable.NONE,
        left_padding_width=0,
        right_padding_width=1,
        **kwargs
    )

    # Set these explicitly due to a bug in prettytable where
    # '0' values are not honored.
    # pylint: disable=protected-access
    tbl._left_padding_width = 0
    tbl._right_padding_width = 2

    def format_table(obj, function):
        """Formats the given object for the given function

        :param object: object to format
        :type object: object
        :param key: value which should be checked
        :type key: string
        :param function: function to format the cell
        :type function: function
        :rtype: PrettyTable
        """
        return str(function(obj))

    for obj in objs:
        row = [format_table(obj, fields.get(key))
               for key in fields.keys()]
        tbl.add_row(row)

    return tbl


def table(fields, objs, **kwargs):
    """Returns a PrettyTable.  `fields` represents the header schema of
    the table.  `objs` represents the objects to be rendered into
    rows.

    :param fields: An OrderedDict, where each element represents a
                   column.  The key is the column header, and the
                   value is the function that transforms an element of
                   `objs` into a value for that column.
    :type fields: OrderdDict(str, function)
    :param objs: objects to render into rows
    :type objs: [object]
    :param **kwargs: kwargs to pass to `prettytable.PrettyTable`
    :type **kwargs: dict
    :rtype: PrettyTable
    """

    return truncate_table(fields, objs, **kwargs)
