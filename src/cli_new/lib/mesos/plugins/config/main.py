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
The config plugin.
"""

import collections

from mesos import config
from mesos.plugins import PluginBase
from mesos.exceptions import CLIException, MesosException


PLUGIN_CLASS = "Config"
PLUGIN_NAME = "config"

VERSION = "Mesos CLI Config Plugin 0.1"

SHORT_HELP = "Config specific commands for the Mesos CLI"


class Config(PluginBase):
    """
    The config plugin.
    """

    COMMANDS = {
        "set" : {
            "alias": "set",
            "arguments" : ["<name>", "<value>"],
            "flags" : {},
            "short_help" : "Set config variable",
            "long_help"  : """\
                Set config variable.
                """
        },
        "unset" : {
            "alias": "unset",
            "arguments" : ["<name>"],
            "flags" : {},
            "short_help" : "Unset config variable",
            "long_help"  : """\
                Unset config variable.
                """
        },
        "show": {
            "alias": "show",
            "arguments": ["[<name>]"],
            "flags": {},
            "short_help": "Shows config variables",
            "long_help": """\
                    Shows config variables.
                    """
        },
    }

    def set(self, argv):
        """
        Sets config variables
        """
        name = argv["<name>"]
        value = argv["<value>"]

        try:
            _, msg = config.set_val(name, value)
        except MesosException as err:
            raise CLIException(err)

        print msg

    def unset(self, argv):
        """
        Unsets config variables
        """
        name = argv["<name>"]
        try:
            msg = config.unset(name)
        except MesosException as err:
            raise CLIException(err)

        print msg

    def show(self, argv):
        """
        Shows config variables
        """
        name = argv["<name>"]
        toml_config = config.get_config(True)

        if name is not None:
            value = toml_config.get(name)
            if value is None:
                raise CLIException("Property {!r} doesn't exist".format(name))
            elif isinstance(value, collections.Mapping):
                raise CLIException(config.generate_choice_msg(name, value))
            else:
                print value
        else:
            # Let's list all of the values
            for key, value in sorted(toml_config.property_items()):
                print '{} {}'.format(key, value)
