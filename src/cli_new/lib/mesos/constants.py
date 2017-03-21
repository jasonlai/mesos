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
Constants used by the CLI
"""

MESOS_DIR = ".mesos"
"""DC/OS data directory.  Can store subcommands and the config file."""

MESOS_SUBCOMMAND_ENV_SUBDIR = 'env'
"""In a package's directory, this is the cli contents subdirectory."""

MESOS_SUBCOMMAND_SUBDIR = 'subcommands'
"""Name of the subdirectory that contains all of the subcommands. This is
relative to the location of the executable."""

MESOS_CONFIG_ENV = 'MESOS_CONFIG'
"""Name of the environment variable pointing to the DC/OS config."""

MESOS_LOG_LEVEL_ENV = 'MESOS_LOG_LEVEL'
"""Name of the environment variable for the DC/OS log level"""

MESOS_DEBUG_ENV = 'MESOS_DEBUG'
"""Name of the environment variable to enable DC/OS debug messages"""

MESOS_PAGER_COMMAND_ENV = 'PAGER'
"""Command to use to page long command output (e.g. 'less -R')"""

PATH_ENV = 'PATH'
"""Name of the environment variable pointing to the executable directories."""

MESOS_COMMAND_PREFIX = 'mesos-'
"""Prefix for all the DC/OS CLI commands."""

VALID_LOG_LEVEL_VALUES = ['debug', 'info', 'warning', 'error', 'critical']
"""List of all the supported log level values for the CLIs"""
