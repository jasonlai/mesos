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
The task plugin.
"""

import posixpath

import six

from mesos import emitting, util, log
from mesos.tables import ls_long_table, task_table
from mesos.plugins import PluginBase
from mesos.client import MesosClient, MesosFile, TaskIO
from mesos.exceptions import (
    MesosException,
    MesosHTTPException,
    DefaultError,
)


PLUGIN_CLASS = "Task"
PLUGIN_NAME = "task"

VERSION = "Mesos CLI Task Plugin 0.1"

SHORT_HELP = "Task specific commands for the Mesos CLI"

EMITTER = emitting.FlatEmitter()


def _mesos_files(tasks, file_, client):
    """Return MesosFile objects for the specified tasks and file name.
    Only include files that satisfy all of the following:

    a) belong to an available slave
    b) have an executor entry on the slave

    :param tasks: tasks on which files reside
    :type tasks: [mesos.Task]
    :param file_: file path to read
    :type file_: str
    :param client: DC/OS client
    :type client: mesos.DCOSClient
    :returns: MesosFile objects
    :rtype: [mesos.MesosFile]
    """

    # load slave state in parallel
    slaves = _load_slaves_state([task.slave() for task in tasks])

    # some completed tasks may have entries on the master, but none on
    # the slave.  since we need the slave entry to get the executor
    # sandbox, we only include files with an executor entry.
    available_tasks = [task for task in tasks
                       if task.slave() in slaves and task.executor()]

    # create files.
    return [MesosFile(file_, task=task, mesos_client=client)
            for task in available_tasks]


def _load_slaves_state(slaves):
    """Fetch each slave's state.json in parallel, and return the reachable
    slaves.

    :param slaves: slaves to fetch
    :type slaves: [MesosSlave]
    :returns: MesosSlave objects that were successfully reached
    :rtype: [MesosSlave]
    """

    reachable_slaves = []

    for job, slave in util.stream(lambda slave: slave.state(), slaves):
        try:
            job.result()
            reachable_slaves.append(slave)
        except MesosException as err:
            EMITTER.publish(
                DefaultError('Error accessing slave: {0}'.format(err)))

    return reachable_slaves


class Task(PluginBase):
    """
    The task plugin.
    """

    COMMANDS = {
        "list" : {
            "alias": "list",
            "arguments" : ["[<filter>]"],
            "flags" : {
                "--completed": "Print completed and in-progress tasks.",
                "--json": "Print json.",
            },
            "short_help" : "List tasks running in mesos",
            "long_help"  : """\
                List tasks running in mesos.
                """
        },
        "ls": {
            "alias": "ls",
            "arguments": ["[<task>]", "[<path>]"],
            "flags": {
                "--completed": "Include completed and in-progress tasks.",
                "--long": "Print full Mesos sandbox file attributes.",
            },
            "short_help": "Print the list of files in the Mesos task sandbox.",
            "long_help": """\
                    Print the list of files in the Mesos task sandbox.
                    """
        },
        "log": {
            "alias": "log",
            "arguments": ["[<task>]", "[<file>]"],
            "flags": {
                "--completed": "Include completed and in-progress tasks.",
                "--follow": "Dynamically update the log.",
                "--lines=N": "Print the last N lines. The default is 10 lines.",
            },
            "short_help": "Print the task log.",
            "long_help": """\
                        Print the task log. By default, the 10 most recent task
                        logs from stdout are printed.
                        """
        },
        "exec": {
            "alias": "exec_",
            "arguments": ["<task>", "<cmd>", "[<args>...]"],
            "flags": {
                "-i --interactive": "Attach a STDIN stream to the remote"
                                    " command for an interactive session.",
                "-t --tty": "Attach a tty to the remote stream.",
            },
            "short_help": "Launch a process (<cmd>) inside of a container for a"
                          " task (<task>).",
            "long_help": """\
                            Launch a process (<cmd>) inside of a container for a
                            task (<task>).
                            """
        },
    }

    def list(self, argv):
        """
        List tasks
        """
        filter_ = argv["<filter>"]
        client = MesosClient()
        completed = argv["--completed"]
        json_ = argv["--json"]
        tasks = sorted(client.get_master().tasks(completed=completed,
                                                 filter_=filter_),
                       key=lambda t: t['name'])
        if json_:
            EMITTER.publish([t.dict() for t in tasks])
        else:
            table = task_table(tasks)
            output = six.text_type(table)
            if output:
                EMITTER.publish(output)

    def ls(self, argv):  # pylint: disable=invalid-name
        """
        List files in Mesos sandbox
        """
        task = argv["<task>"]
        path = argv["<path>"]
        long_ = argv["--long"]
        completed = argv["--completed"]
        if path is None:
            path = '.'

        if path.startswith('/'):
            path = path[1:]

        mesos_client = MesosClient()
        task_objects = mesos_client.get_master().tasks(
            filter_=task,
            completed=completed)

        if len(task_objects) == 0:
            if task is None:
                raise MesosException("No tasks found")

            raise MesosException(
                'Cannot find a task with ID containing "{}"'.format(task))

        try:
            all_files = []
            for task_obj in task_objects:
                dir_ = posixpath.join(task_obj.directory(), path)
                all_files += [(task_obj['id'],
                               mesos_client.browse(task_obj.slave(), dir_))]
        except MesosHTTPException as err:
            if err.response.status_code == 404:
                raise MesosException(
                    'Cannot access [{}]: No such file or directory'
                    .format(path))
            else:
                raise

        add_header = len(all_files) > 1
        for (task_id, files) in all_files:
            if add_header:
                EMITTER.publish('===> {} <==='.format(task_id))
            if long_:
                EMITTER.publish(ls_long_table(files))
            else:
                EMITTER.publish(
                    '  '.join(posixpath.basename(file_['path'])
                              for file_ in files))

    def log(self, argv):
        """
        Print the task log
        """
        task = argv["<task>"]
        file_ = argv["<file>"]
        completed = argv["--completed"]
        follow = argv["--follow"]
        lines = argv["--lines"]

        if file_ is None:
            file_ = 'stdout'

        if lines is None:
            lines = 10
        lines = util.parse_int(lines)

        # get tasks
        client = MesosClient()
        master = client.get_master()
        tasks = master.tasks(completed=completed, filter_=task)

        if not tasks:
            if not task:
                raise MesosException("No tasks found. Exiting.")
            elif not completed:
                completed_tasks = master.tasks(completed=True, filter_=task)
                if completed_tasks:
                    msg = 'No running tasks match ID [{}]; however, there '\
                        .format(task)
                    if len(completed_tasks) > 1:
                        msg += 'are {} matching completed tasks. '.format(
                            len(completed_tasks))
                    else:
                        msg += 'is 1 matching completed task. '
                    msg += 'Run with --completed to see these logs.'
                    raise MesosException(msg)
            raise MesosException('No matching tasks. Exiting.')

        mesos_files = _mesos_files(tasks, file_, client)
        if not mesos_files:
            if task is None:
                msg = "No tasks found. Exiting."
            else:
                msg = "No matching tasks. Exiting."
            raise MesosException(msg)

        log.log_files(mesos_files, follow, lines)

        return 0

    def exec_(self, argv):
        """
        Launch a process inside a container with the given <task_id>
        """
        task = argv["<task>"]
        cmd = argv["<cmd>"]
        args = argv["<args>"]
        interactive = argv["--interactive"]
        tty = argv["--tty"]

        task_io = TaskIO(task, cmd, args, interactive, tty)
        task_io.run()
        return 0
