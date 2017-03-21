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

# pylint: disable=too-many-lines,too-many-instance-attributes,too-many-arguments

"""
Mesos REST Client
"""

from __future__ import absolute_import

import base64
import fnmatch
import itertools
import json
import os
import signal
import sys
import uuid
import threading
import time
import tty

from functools import partial
from Queue import Queue

# termios must be imported last
import termios

from six.moves import urllib

from mesos import config
from mesos import http
from mesos import recordio
from mesos import util
from mesos.exceptions import (
    MesosMissingConfigException,
    MesosHTTPException,
    MesosException,
)

LOGGER = util.get_logger(__name__)


class MesosClient(object):
    """Client for communicating with DC/OS"""

    def __init__(self):
        toml_config = config.get_config()

        self._mesos_master_url = config.get_config_val(
            'core.mesos_master_url', toml_config)
        if self._mesos_master_url is None:
            raise MesosMissingConfigException(['core.mesos_master_url'])

        self._timeout = config.get_config_val('core.timeout', toml_config)

    def master_url(self, path):
        """ Create a master URL

        :param path: the path suffix of the desired URL
        :type path: str
        :returns: URL that hits the master
        :rtype: str
        """
        return urllib.parse.urljoin(self._mesos_master_url, path)

    def slave_url(self, private_url, path):
        """Create a slave URL

        :param private_url: The slave's private URL derived from its
                            pid.  Used when we're accessing mesos
                            directly.
        :type private_url: str
        :param path: the path suffix of the desired URL
        :type path: str
        :returns: URL that hits the master
        :rtype: str

        """
        return urllib.parse.urljoin(private_url, path)

    def get_master(self):
        """Create a Master object using the url stored in the
        'core.mesos_master_url' property if it exists.  Otherwise, we use
        the `core.dcos_url` property

        :param client: MesosClient
        :type client: MesosClient | None
        :returns: master state object
        :rtype: Master
        """
        return Master(self.get_master_state())

    def get_master_state(self):
        """Get the Mesos master state json object

        :returns: Mesos' master state json object
        :rtype: dict
        """

        url = self.master_url('master/state.json')
        return http.get(url, timeout=self._timeout).json()

    def get_slave_state(self, private_url):
        """Get the Mesos slave state json object

        :param private_url: The slave's private URL derived from its
                            pid.  Used when we're accessing mesos
                            directly, rather than through DC/OS.
        :type private_url: str
        :returns: Mesos' master state json object
        :rtype: dict

        """

        url = self.slave_url(private_url, 'state.json')
        return http.get(url, timeout=self._timeout).json()

    def get_state_summary(self):
        """Get the Mesos master state summary json object

        :returns: Mesos' master state summary json object
        :rtype: dict
        """

        url = self.master_url('master/state-summary')
        return http.get(url, timeout=self._timeout).json()

    def slave_file_read(self, private_url, path, offset, length):
        """See the master_file_read() docs

        :param slave_id: slave ID
        :type slave_id: str
        :param path: absolute path to read
        :type path: str
        :param private_url: The slave's private URL derived from its
                            pid.  Used when we're accessing mesos
                            directly, rather than through DC/OS.
        :type private_url: str
        :param offset: start byte location, or -1.  -1 means read no data, and
                       is used to fetch the size of the file in the response's
                       'offset' parameter.
        :type offset: int
        :param length: number of bytes to read, or -1.  -1 means read the whole
                       file
        :type length: int
        :returns: files/read.json response
        :rtype: dict

        """

        url = self.slave_url(private_url, 'files/read.json')
        params = {'path': path,
                  'length': length,
                  'offset': offset}
        return http.get(url, params=params, timeout=self._timeout).json()

    def master_file_read(self, path, length, offset):
        """This endpoint isn't well documented anywhere, so here is the spec
        derived from the mesos source code:

        request format:
        {
            path: absolute path to read
            offset: start byte location, or -1.  -1 means read no data, and
                    is used to fetch the size of the file in the response's
                    'offset' parameter.
            length: number of bytes to read, or -1.  -1 means read the whole
                    file.
        }

        response format:
        {
            data: file data.  Empty if a request.offset=-1.  Could be
                  smaller than request.length if EOF was reached, or if (I
                  believe) request.length is larger than the length
                  supported by the server (16 pages I believe).

            offset: the offset value from the request, or the size of the
                    file if the request offset was -1 or >= the file size.
        }

        :param path: absolute path to read
        :type path: str
        :param offset: start byte location, or -1.  -1 means read no data, and
                       is used to fetch the size of the file in the response's
                       'offset' parameter.
        :type offset: int
        :param length: number of bytes to read, or -1.  -1 means read the whole
                       file
        :type length: int
        :returns: files/read.json response
        :rtype: dict
        """

        url = self.master_url('files/read.json')
        params = {'path': path,
                  'length': length,
                  'offset': offset}
        return http.get(url, params=params, timeout=self._timeout).json()

    def shutdown_framework(self, framework_id):
        """Shuts down a Mesos framework

        :param framework_id: ID of the framework to shutdown
        :type framework_id: str
        :returns: None
        """

        LOGGER.info('Shutting down framework %s', framework_id)

        data = 'frameworkId={}'.format(framework_id)

        url = self.master_url('master/teardown')

        # In Mesos 0.24, /shutdown was removed.
        # If /teardown doesn't exist, we try /shutdown.
        try:
            http.post(url, data=data, timeout=self._timeout)
        except MesosHTTPException as err:
            if err.response.status_code == 404:
                url = self.master_url('master/shutdown')
                http.post(url, data=data, timeout=self._timeout)
            else:
                raise

    def browse(self, slave, path):
        """ GET /files/browse.json

        Request
        path:...  # path to run ls on

        Response
        [
          {
            path:  # full path to file
            nlink:
            size:
            mtime:
            mode:
            uid:
            gid:
          }
        ]

        :param slave: slave to issue the request on
        :type slave: Slave
        :returns: /files/browse.json response
        :rtype: dict
        """

        url = self.slave_url(slave.http_url(), 'files/browse.json')
        return http.get(url, params={'path': path}).json()


class Master(object):
    """Mesos Master Model


    :param state: Mesos master's state.json
    :type state: dict
    """

    def __init__(self, state):
        self._state = state
        self._frameworks = {}
        self._slaves = {}

    def state(self):
        """Returns master's master/state.json.

        :returns: state.json
        :rtype: dict
        """
        return self._state

    def slave(self, fltr):
        """Returns the slave that has `fltr` in its ID. If any slaves
        are an exact match, returns that task, id not raises a
        MesosException if there is not exactly one such slave.

        :param fltr: filter string
        :type fltr: str
        :returns: the slave that has `fltr` in its ID
        :rtype: Slave
        """

        slaves = self.slaves(fltr)

        if len(slaves) == 0:
            raise MesosException('No slave found with ID "{}".'.format(fltr))

        elif len(slaves) > 1:

            exact_matches = [s for s in slaves if s['id'] == fltr]
            if len(exact_matches) == 1:
                return exact_matches[0]

            else:
                matches = ['\t{0}'.format(s['id']) for s in slaves]
                raise MesosException(
                    "There are multiple slaves with that ID. " +
                    "Please choose one:\n{}".format('\n'.join(matches)))

        else:
            return slaves[0]

    def task(self, fltr, completed=False):
        """Returns the task with `fltr` in its ID.  Raises a MesosException if
        there is not exactly one such task.

        :param fltr: filter string
        :type fltr: str
        :returns: the task that has `fltr` in its ID
        :param completed: also include completed tasks
        :type completed: bool
        :rtype: Task
        """

        tasks = self.tasks(fltr, completed)

        if len(tasks) == 0:
            raise MesosException(
                'Cannot find a task with ID containing "{}"'.format(fltr))

        elif len(tasks) > 1:
            msg = [("There are multiple tasks with ID matching [{}]. " +
                    "Please choose one:").format(fltr)]
            msg += ["\t{0}".format(t["id"]) for t in tasks]
            raise MesosException('\n'.join(msg))

        else:
            return tasks[0]

    def framework(self, framework_id):
        """Returns a framework by ID

        :param framework_id: the framework's ID
        :type framework_id: str
        :returns: the framework
        :rtype: Framework
        """

        for f in self._framework_dicts(True, True):
            if f['id'] == framework_id:
                return self._framework_obj(f)
        return None

    def slaves(self, fltr=""):
        """Returns those slaves that have `fltr` in their 'id'

        :param fltr: filter string
        :type fltr: str
        :returns: Those slaves that have `fltr` in their 'id'
        :rtype: [Slave]
        """

        return [self._slave_obj(slave)
                for slave in self.state()['slaves']
                if fltr in slave['id']]

    def tasks(self, filter_=None, completed=False):
        """Returns tasks running under the master

        :param filter_: May be None, a substring or regex. None
                        returns all tasks, else return tasks whose
                        'id' matches `fltr`.
        :type filter_: str | None
        :param completed: also include completed tasks
        :type completed: bool
        :returns: a list of tasks
        :rtype: [Task]
        """

        keys = ['tasks']
        if completed:
            keys.extend(['completed_tasks'])

        tasks = []
        for framework in self._framework_dicts(completed, completed):
            for task in _merge(framework, keys):
                if filter_ is None or \
                                filter_ in task['id'] or \
                        fnmatch.fnmatchcase(task['id'], filter_):
                    task = self._framework_obj(framework).task(task['id'])
                    tasks.append(task)

        return tasks

    def get_container_id(self, task_id):
        """Returns the container ID for a task ID matching `task_id`

        :param task_id: The task ID which will be mapped to container ID
        :type task_id: str
        :returns: The container ID associated with 'task_id'
        :rtype: str
        """

        def _get_task(task_id):
            candidates = []
            if 'frameworks' in self.state():
                for framework in self.state()['frameworks']:
                    if 'tasks' not in framework:
                        continue
                    for task in framework['tasks']:
                        if 'id' not in task:
                            continue
                        if task['id'].startswith(task_id):
                            candidates.append(task)

            if len(candidates) == 1:
                return candidates[0]

            raise MesosException(
                "More than one task matching '{}' found: {}"
                .format(task_id, candidates))

        def _get_container_status(task):
            if 'statuses' in task:
                if len(task['statuses']) > 0:
                    if 'container_status' in task['statuses'][0]:
                        return task['statuses'][0]['container_status']

            raise MesosException(
                "Unable to obtain container status for task '{}'"
                .format(task['id']))

        def _get_container_id(container_status):
            if 'container_id' in container_status:
                if 'value' in container_status['container_id']:
                    return container_status['container_id']['value']

            raise MesosException(
                "No container found for the specified task."
                " It might still be spinning up."
                " Please try again.")

        if not task_id:
            raise MesosException("Invalid task ID")

        task = _get_task(task_id)
        container_status = _get_container_status(task)
        return _get_container_id(container_status)

    def frameworks(self, inactive=False, completed=False):
        """Returns a list of all frameworks

        :param inactive: also include inactive frameworks
        :type inactive: bool
        :param completed: also include completed frameworks
        :type completed: bool
        :returns: a list of frameworks
        :rtype: [Framework]
        """

        return [self._framework_obj(framework)
                for framework in self._framework_dicts(inactive, completed)]

    def _slave_obj(self, slave):
        """Returns the Slave object corresponding to the provided `slave`
        dict.  Creates it if it doesn't exist already.

        :param slave: slave
        :type slave: dict
        :returns: Slave
        :rtype: Slave
        """

        if slave['id'] not in self._slaves:
            self._slaves[slave['id']] = Slave(slave, None, self)
        return self._slaves[slave['id']]

    def _framework_obj(self, framework):
        """Returns the Framework object corresponding to the provided
         `framework` dict.  Creates it if it doesn't exist already.

        :param framework: framework
        :type framework: dict
        :returns: Framework
        :rtype: Framework
        """

        if framework['id'] not in self._frameworks:
            self._frameworks[framework['id']] = Framework(framework, self)
        return self._frameworks[framework['id']]

    def _framework_dicts(self, inactive=False, completed=False):
        """Returns a list of all frameworks as their raw dictionaries

        :param inactive: also include inactive frameworks
        :type inactive: bool
        :param completed: also include completed frameworks
        :type completed: bool
        :returns: a list of frameworks
        """

        if completed:
            for framework in self.state()['completed_frameworks']:
                yield framework

        for framework in self.state()['frameworks']:
            if inactive or framework['active']:
                yield framework


class Slave(object):
    """Mesos Slave Model

    :param short_state: slave's entry from the master's state.json
    :type short_state: dict
    :param state: slave's state.json
    :type state: dict | None
    :param master: slave's master
    :type master: Master
    """

    def __init__(self, short_state, state, master):
        self._short_state = short_state
        self._state = state
        self._master = master

    def state(self):
        """Get the slave's state.json object.  Fetch it if it's not already
        an instance variable.

        :returns: This slave's state.json object
        :rtype: dict
        """

        if not self._state:
            self._state = MesosClient().get_slave_state(self.http_url())
        return self._state

    def http_url(self):
        """
        :returns: The private HTTP URL of the slave.  Derived from the
                  `pid` property.
        :rtype: str
        """

        parsed_pid = parse_pid(self['pid'])
        return 'http://{}:{}'.format(parsed_pid[1], parsed_pid[2])

    def _framework_dicts(self):
        """Returns the framework dictionaries from the state.json dict

        :returns: frameworks
        :rtype: [dict]
        """

        return _merge(self.state(), ['frameworks', 'completed_frameworks'])

    def executor_dicts(self):
        """Returns the executor dictionaries from the state.json

        :returns: executors
        :rtype: [dict]
        """

        iters = [_merge(framework, ['executors', 'completed_executors'])
                 for framework in self._framework_dicts()]
        return itertools.chain(*iters)

    def __getitem__(self, name):
        """Support the slave[attr] syntax

        :param name: attribute to get
        :type name: str
        :returns: the value for this attribute in the underlying
                  slave dictionary
        :rtype: object
        """

        return self._short_state[name]


class Framework(object):
    """ Mesos Framework Model

    :param framework: framework properties
    :type framework: dict
    :param master: framework's master
    :type master: Master
    """

    def __init__(self, framework, master):
        self._framework = framework
        self._master = master
        self._tasks = {}  # id->Task map

    def task(self, task_id):
        """Returns a task by id

        :param task_id: the task's id
        :type task_id: str
        :returns: the task
        :rtype: Task
        """

        for task in _merge(self._framework, ['tasks', 'completed_tasks']):
            if task['id'] == task_id:
                return self._task_obj(task)
        return None

    def _task_obj(self, task):
        """Returns the Task object corresponding to the provided `task`
        dict.  Creates it if it doesn't exist already.

        :param task: task
        :type task: dict
        :returns: Task
        :rtype: Task
        """

        if task['id'] not in self._tasks:
            self._tasks[task['id']] = Task(task, self._master)
        return self._tasks[task['id']]

    def dict(self):
        """Return raw dict"""
        return self._framework

    def __getitem__(self, name):
        """Support the framework[attr] syntax

        :param name: attribute to get
        :type name: str
        :returns: the value for this attribute in the underlying
                  framework dictionary
        :rtype: object
        """

        return self._framework[name]


class Task(object):
    """Mesos Task Model.

    :param task: task properties
    :type task: dict
    :param master: mesos master
    :type master: Master
    """

    def __init__(self, task, master):
        self._task = task
        self._master = master

    def dict(self):
        """
        :returns: dictionary representation of this Task
        :rtype: dict
        """

        return self._task

    def framework(self):
        """Returns this task's framework

        :returns: task's framework
        :rtype: Framework
        """

        return self._master.framework(self["framework_id"])

    def slave(self):
        """Returns the task's slave

        :returns: task's slave
        :rtype: Slave
        """

        return self._master.slave(self["slave_id"])

    def user(self):
        """Task owner

        :returns: task owner
        :rtype: str
        """

        return self.framework()['user']

    def executor(self):
        """ Returns this tasks' executor

        :returns: task's executor
        :rtype: dict
        """
        for executor in self.slave().executor_dicts():
            tasks = _merge(executor,
                           ['completed_tasks',
                            'tasks',
                            'queued_tasks'])
            if any(task['id'] == self['id'] for task in tasks):
                return executor
        return None

    def directory(self):
        """ Sandbox directory for this task

        :returns: path to task's sandbox
        :rtype: str
        """

        return self.executor()['directory']

    def __getitem__(self, name):
        """Support the task[attr] syntax

        :param name: attribute to get
        :type name: str
        :returns: the value for this attribute in the underlying
                  task dictionary
        :rtype: object
        """

        return self._task[name]


def parse_pid(pid):
    """ Parse the mesos pid string,

    :param pid: pid of the form "id@ip:port"
    :type pid: str
    :returns: (id, ip, port)
    :rtype: (str, str, str)
    """

    id_, second = pid.split('@')
    ip, port = second.split(':')
    return id_, ip, port


def _merge(dict_, keys):
    """ Merge multiple lists from a dictionary into one iterator.
        e.g. _merge({'a': [1, 2], 'b': [3]}, ['a', 'b']) ->
             iter(1, 2, 3)

    :param dict_: dictionary
    :type dict_: dict
    :param keys: keys to merge
    :type keys: [hashable]
    :returns: iterator
    :rtype: iter
    """

    return itertools.chain(*[dict_[k] for k in keys])


class MesosFile(object):
    """File-like object that is backed by a remote slave or master file.
    Uses the files/read.json endpoint.

    If `task` is provided, the file host is `task.slave()`.  If
    `slave` is provided, the file host is `slave`.  It is invalid to
    provide both.  If neither is provided, the file host is the
    leading master.

    :param path: file's path, relative to the sandbox if `task` is given
    :type path: str
    :param task: file's task
    :type task: Task | None
    :param slave: slave where the file lives
    :type slave: Slave | None
    :param dcos_client: client to use for network requests
    :type dcos_client: DCOSClient | None

    """

    def __init__(self, path, task=None, slave=None, mesos_client=None):
        if task and slave:
            raise ValueError(
                "You cannot provide both `task` and `slave` " +
                "arguments.  `slave` is understood to be `task.slave()`")

        if slave:
            self._slave = slave
        elif task:
            self._slave = task.slave()
        else:
            self._slave = None

        self._task = task
        self._path = path
        self._mesos_client = mesos_client or MesosClient()
        self._cursor = 0

    def size(self):
        """Size of the file

        :returns: size of the file
        :rtype: int
        """

        params = self._params(0, offset=-1)
        return self._fetch(params)["offset"]

    def seek(self, offset, whence=os.SEEK_SET):
        """Seek to the provided location in the file.

        :param offset: location to seek to
        :type offset: int
        :param whence: determines whether `offset` represents a
                       location that is absolute, relative to the
                       beginning of the file, or relative to the end
                       of the file
        :type whence: os.SEEK_SET | os.SEEK_CUR | os.SEEK_END
        :returns: None
        :rtype: None
        """

        if whence == os.SEEK_SET:
            self._cursor = 0 + offset
        elif whence == os.SEEK_CUR:
            self._cursor += offset
        elif whence == os.SEEK_END:
            self._cursor = self.size() + offset
        else:
            raise ValueError(
                "Unexpected value for `whence`: {}".format(whence))

    def tell(self):
        """ The current cursor position.

        :returns: the current cursor position
        :rtype: int
        """

        return self._cursor

    def read(self, length=None):
        """Reads up to `length` bytes, or the entire file if `length` is None.

        :param length: number of bytes to read
        :type length: int | None
        :returns: data read
        :rtype: str
        """

        data = ''
        while length is None or length - len(data) > 0:
            chunk_length = -1 if length is None else length - len(data)
            chunk = self._fetch_chunk(chunk_length)
            if chunk == '':
                break
            data += chunk

        return data

    def _host_path(self):
        """ The absolute path to the file on slave.

        :returns: the absolute path to the file on slave
        :rtype: str
        """

        if self._task:
            directory = self._task.directory().rstrip('/')
            executor = self._task.executor()
            # executor.type is currently used only by pods. All tasks in a pod
            # share an executor, so if this is a pod, get the task logs instead
            # of the executor logs
            if executor.get('type') == "DEFAULT":
                task_id = self._task.dict().get('id')
                return directory + '/tasks/{}/'.format(task_id) + self._path
            else:
                return directory + '/' + self._path
        else:
            return self._path

    def _params(self, length, offset=None):
        """GET parameters to send to files/read.json.  See the MesosFile
        docstring for full information.

        :param length: number of bytes to read
        :type length: int
        :param offset: start location.  if None, will use the location
                       of the current file cursor
        :type offset: int
        :returns: GET parameters
        :rtype: dict
        """

        if offset is None:
            offset = self._cursor

        return {
            'path': self._host_path(),
            'offset': offset,
            'length': length
        }

    def _fetch_chunk(self, length, offset=None):
        """Fetch data from files/read.json

        :param length: number of bytes to fetch
        :type length: int
        :param offset: start location.  If not None, this file's
                       cursor is set to `offset`
        :type offset: int
        :returns: data read
        :rtype: str
        """

        if offset is not None:
            self.seek(offset, os.SEEK_SET)

        params = self._params(length)
        data = self._fetch(params)["data"]
        self.seek(len(data), os.SEEK_CUR)
        return data

    def _fetch(self, params):
        """Fetch data from files/read.json

        :param params: GET parameters
        :type params: dict
        :returns: response dict
        :rtype: dict
        """

        if self._slave:
            return self._mesos_client.slave_file_read(self._slave.http_url(),
                                                      **params)
        else:
            return self._mesos_client.master_file_read(**params)

    def __str__(self):
        """String representation of the file: <task_id:file_path>

        :returns: string representation of the file
        :rtype: str
        """

        if self._task:
            return "task:{0}:{1}".format(self._task['id'], self._path)
        elif self._slave:
            return "slave:{0}:{1}".format(self._slave['id'], self._path)
        else:
            return "master:{0}".format(self._path)


class TaskIO(object):
    """Object used to stream I/O between a
    running Mesos task and the local terminal.

    :param task: task ID
    :type task: str
    :param cmd: a command to launch inside the task's container
    :type cmd: str
    :param args: Additional arguments for the command
    :type args: str
    :param interactive: whether to attach STDIN of the current
                        terminal to the new command being launched
    :type interactive: bool
    :param tty: whether to allocate a tty for this command and attach
                the local terminal to it
    :type tty: bool
    """

    # The interval to send heartbeat messages to
    # keep persistent connections alive.
    HEARTBEAT_INTERVAL = 30
    HEARTBEAT_INTERVAL_NANOSECONDS = HEARTBEAT_INTERVAL * 1000000000

    def __init__(self, task_id, cmd=None, args=None,
                 interactive=False, use_tty=False):
        # Store relevant parameters of the call for later.
        self.cmd = cmd
        self.interactive = interactive
        self.tty = use_tty
        self.args = args

        # Create a client and grab a reference to the DC/OS master.
        client = MesosClient()
        master = client.get_master()

        # Get the URL to the agent running the task.
        task_obj = master.task(task_id)
        if client._mesos_master_url:  # pylint: disable=protected-access
            self.agent_url = client.slave_url(
                private_url=task_obj.slave().http_url(),
                path="api/v1")
        else:
            self.agent_url = client.slave_url(
                private_url="",
                path="api/v1")

        # Grab a reference to the container ID for the task.
        self.parent_id = master.get_container_id(task_id)

        # Generate a new UUID for the nested container
        # used to run commands passed to `task exec`.
        self.container_id = str(uuid.uuid4())

        # Set up a recordio encoder and decoder
        # for any incoming and outgoing messages.
        self.encoder = recordio.Encoder(
            lambda s: bytearray(json.dumps(s, ensure_ascii=False), "UTF-8"))
        self.decoder = recordio.Decoder(
            lambda s: json.loads(s.decode("UTF-8")))

        # Set up queues to send messages between threads used for
        # reading/writing to STDIN/STDOUT/STDERR and threads
        # sending/receiving data over the network.
        self.input_queue = Queue()
        self.output_queue = Queue()

        # Set up an event to block attaching
        # input until attaching output is complete.
        self.attach_input_event = threading.Event()
        self.attach_input_event.clear()

        # Set up an event to block printing the output
        # until an attach input event has successfully
        # been established.
        self.print_output_event = threading.Event()
        self.print_output_event.clear()

        # Set up an event to block the main thread
        # from exiting until signaled to do so.
        self.exit_event = threading.Event()
        self.exit_event.clear()

        # Use a class variable to store exceptions thrown on
        # other threads and raise them on the main thread before
        # exiting.
        self.exception = None

    def run(self):
        """Run the helper threads in this class which enable streaming
        of STDIN/STDOUT/STDERR between the CLI and the Mesos Agent API.

        If a tty is requested, we take over the current terminal and
        put it into raw mode. We make sure to reset the terminal back
        to its original settings before exiting.
        """

        # Without a TTY.
        if not self.tty:
            try:
                self._start_threads()
                self.exit_event.wait()
            except Exception as err:
                self.exception = err

            if self.exception:
                raise self.exception  # pylint: disable=raising-bad-type
            return

        # With a TTY.
        if util.is_windows_platform():
            raise MesosException(
                "Running with the '--tty' flag is not supported on windows.")

        if not sys.stdin.isatty():
            raise MesosException(
                "Must be running in a tty to pass the '--tty flag'.")

        fno = sys.stdin.fileno()
        oldtermios = termios.tcgetattr(fno)

        try:
            if self.interactive:
                tty.setraw(fno, when=termios.TCSANOW)
                self._window_resize(signal.SIGWINCH, None)
                signal.signal(signal.SIGWINCH, self._window_resize)

            self._start_threads()
            self.exit_event.wait()
        except Exception as err:
            self.exception = err

        termios.tcsetattr(
            sys.stdin.fileno(),
            termios.TCSAFLUSH,
            oldtermios)

        if self.exception:
            raise self.exception  # pylint: disable=raising-bad-type

    def _thread_wrapper(self, func):
        """A wrapper around all threads used in this class

        If a thread throws an exception, it will unblock the main
        thread and save the exception in a class variable. The main
        thread will then rethrow the exception before exiting.

        :param func: The start function for the thread
        :type func: function
        """
        try:
            func()
        except Exception as err:
            self.exception = err
            self.exit_event.set()

    def _start_threads(self):
        """Start all threads associated with this class
        """
        if self.interactive:
            # Collects input from STDIN and puts
            # it in the input_queue as data messages.
            thread = threading.Thread(
                target=self._thread_wrapper,
                args=(self._input_thread,))
            thread.daemon = True
            thread.start()

            # Prepares heartbeat control messages and
            # puts them in the input queueaat a specific
            # heartbeat interval.
            thread = threading.Thread(
                target=self._thread_wrapper,
                args=(self._heartbeat_thread,))
            thread.daemon = True
            thread.start()

            # Opens a persistent connection with the mesos agent and
            # feeds it both control and data messages from the input
            # queue via ATTACH_CONTAINER_INPUT messages.
            thread = threading.Thread(
                target=self._thread_wrapper,
                args=(self._attach_container_input,))
            thread.daemon = True
            thread.start()

        # Opens a persistent connection with a mesos agent, reads
        # data messages from it and feeds them to an output_queue.
        thread = threading.Thread(
            target=self._thread_wrapper,
            args=(self._launch_nested_container_session,))
        thread.daemon = True
        thread.start()

        # Collects data messages from the output queue and writes
        # their content to STDOUT and STDERR.
        thread = threading.Thread(
            target=self._thread_wrapper,
            args=(self._output_thread,))
        thread.daemon = True
        thread.start()

    def _launch_nested_container_session(self):  # pylint: disable=invalid-name
        """Sends a request to the Mesos Agent to launch a new
        nested container and attach to its output stream.
        The output stream is then sent back in the response.
        """

        message = {
            'type': "LAUNCH_NESTED_CONTAINER_SESSION",
            'launch_nested_container_session': {
                'container_id': {
                    'parent': {
                        'value': self.parent_id
                    },
                    'value': self.container_id
                },
                'command': {
                    'value': self.cmd,
                    'arguments': [self.cmd] + self.args,
                    'shell': False}}}

        if self.tty:
            message['launch_nested_container_session']['container'] = {
                'type': 'MESOS',
                'tty_info': {}
            }

        req_extra_args = {
            'stream': True,
            'headers': {
                'Content-Type': 'application/json',
                'Accept': 'application/recordio',
                'Message-Accept': 'application/json'}}

        response = http.post(
            self.agent_url,
            data=json.dumps(message),
            **req_extra_args)

        self._process_output_stream(response)

    def _process_output_stream(self, response):
        """Gets data streamed over the given response and places the
        returned messages into our output_queue. Only expects to
        receive data messages.

        :param response: Response from an http post
        :type response: requests.models.Response
        """

        # Now that we are ready to process the output stream (meaning
        # our output connection has been established), allow the input
        # stream to be attached by setting an event.
        self.attach_input_event.set()

        # If we are running in interactive mode, wait to make sure that
        # our input connection succeeds before pushing any output to the
        # output queue.
        if self.interactive:
            self.print_output_event.wait()

        try:
            for chunk in response.iter_content(chunk_size=None):
                records = self.decoder.decode(bytearray(chunk))

                for rec in records:
                    if rec.get('type') and rec['type'] == 'DATA':
                        self.output_queue.put(rec['data'])
        except Exception as err:
            raise MesosException(
                "Error parsing output stream: {error}".format(error=err))

        self.output_queue.join()
        self.exit_event.set()

    def _attach_container_input(self):
        """Streams all input data (e.g. STDIN) from the client to the agent
        """

        def _initial_input_streamer():
            """Generator function yielding the initial ATTACH_CONTAINER_INPUT
            message for streaming. We have a separate generator for this so
            that we can attempt the connection once before committing to a
            persistent connection where we stream the rest of the input.

            :returns: A RecordIO encoded message
            """

            message = {
                u'type': u'ATTACH_CONTAINER_INPUT',
                u'attach_container_input': {
                    u'type': u'CONTAINER_ID',
                    u'container_id': {
                        u'parent': {
                            u'value': self.parent_id
                        },
                        u'value': self.container_id}}}

            yield self.encoder.encode(message)

        def _input_streamer():
            """Generator function yielding ATTACH_CONTAINER_INPUT
            messages for streaming. It yields the _intitial_input_streamer()
            message, followed by messages from the input_queue on each
            subsequent call.

            :returns: A RecordIO encoded message
            """

            yield next(_initial_input_streamer())

            while True:
                record = self.input_queue.get()
                if not record:
                    break
                yield record

        req_extra_args = {
            u'headers': {
                u'Content-Type': u'application/recordio',
                u'Message-Content-Type': u'application/json',
                u'Accept': u'application/json',
                u'Connection': u'close',
                u'Transfer-Encoding': u'chunked'
            }
        }

        # Ensure we don't try to attach our input to a container that isn't
        # fully up and running by waiting until the
        # `_process_output_stream` function signals us that it's ready.
        self.attach_input_event.wait()

        # Send an intial "Test" message to ensure that we are able to
        # establish a connection with the agent. If we aren't we will throw
        # an exception and break out of this thread. However, in cases where
        # we receive a 500 response from the agent, we actually want to
        # continue without throwing an exception. A 500 error indicates that
        # we can't connect to the container because it has already finished
        # running. In that case we continue running to allow the output queue
        # to be flushed.
        try:
            http.post(
                self.agent_url,
                data=_initial_input_streamer(),
                **req_extra_args)
        except MesosHTTPException as err:
            if not err.response.status_code == 500:
                raise err

        # If we succeeded with that connection, unblock process_output_stream()
        # from sending output data to the output thread.
        self.print_output_event.set()

        # Begin streaming the input.
        http.post(
            self.agent_url,
            data=_input_streamer(),
            **req_extra_args)

    def _input_thread(self):
        """Reads from STDIN and places a message
        with that data onto the input_queue.
        """

        message = {
            u'type': u'ATTACH_CONTAINER_INPUT',
            u'attach_container_input': {
                u'type': u'PROCESS_IO',
                u'process_io': {
                    u'type': u'DATA',
                    u'data': {
                        u'type': u'STDIN',
                        u'data': u''}}}}

        for chunk in iter(partial(os.read, sys.stdin.fileno(), 1024), b''):
            message[u'attach_container_input'][u'process_io']\
                [u'data'][u'data'] = base64.b64encode(chunk).decode('utf-8')

            self.input_queue.put(self.encoder.encode(message))

        # Push an empty string to indicate EOF to the server and push
        # 'None' to signal that we are done processing input.
        message[u'attach_container_input'][u'process_io']\
            [u'data'][u'data'] = u''
        self.input_queue.put(self.encoder.encode(message))
        self.input_queue.put(None)

    def _output_thread(self):
        """Reads from the output_queue and writes the data
        to the appropriate STDOUT or STDERR.
        """

        while True:
            # Get a message from the output queue and decode it.
            # Then write the data to the appropriate stdout or stderr.
            output = self.output_queue.get()

            if not output.get('data'):
                raise MesosException("Error no 'data' field in output message")

            data = output['data']
            data = base64.b64decode(data.encode('utf-8'))

            if output.get('type') and output['type'] == u'STDOUT':
                try:
                    sys.stdout.buffer.write(data)
                except AttributeError:
                    sys.stdout.write(data)
                sys.stdout.flush()
            elif output.get('type') and output['type'] == u'STDERR':
                try:
                    sys.stderr.buffer.write(data)
                except AttributeError:
                    sys.stderr.write(data)
                sys.stderr.flush()
            else:
                raise MesosException("Unsupported data type in output stream")

            self.output_queue.task_done()

    def _heartbeat_thread(self):
        """Generates a heartbeat message to send over the
        ATTACH_CONTAINER_INPUT stream every `interval` seconds and
        inserts it in the input queue.
        """

        interval = self.HEARTBEAT_INTERVAL
        nanoseconds = self.HEARTBEAT_INTERVAL_NANOSECONDS

        message = {
            u'type': u'ATTACH_CONTAINER_INPUT',
            u'attach_container_input': {
                u'type': u'PROCESS_IO',
                u'process_io': {
                    u'type': u'CONTROL',
                    u'control': {
                        u'type': u'HEARTBEAT',
                        u'heartbeat': {
                            u'interval': {
                                u'nanoseconds': nanoseconds}}}}}}

        while True:
            self.input_queue.put(self.encoder.encode(message))
            time.sleep(interval)

    def _window_resize(self, signum, frame):  # pylint: disable=unused-argument
        """Signal handler for SIGWINCH.

        Generates a message with the current demensions of the
        terminal and puts it in the input_queue.

        :param signum: the signal number being handled
        :type signum: int
        :param frame: current stack frame
        :type frame: frame
        """

        # Determine the size of our terminal, and create the message to be sent
        rows, columns = os.popen('stty size', 'r').read().split()

        message = {
            u'type': u'ATTACH_CONTAINER_INPUT',
            u'attach_container_input': {
                u'type': u'PROCESS_IO',
                u'process_io': {
                    u'type': u'CONTROL',
                    u'control': {
                        u'type': u'TTY_INFO',
                        u'tty_info': {
                            u'window_size': {
                                u'rows': int(rows),
                                u'columns': int(columns)}}}}}}

        self.input_queue.put(self.encoder.encode(message))
