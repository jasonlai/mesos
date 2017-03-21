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
Configuration primitives for read/write access to TOML config files
"""

from __future__ import absolute_import

import collections
import copy
import os

import toml

from mesos import constants, util, jsonitem
from mesos.exceptions import MesosException

LOGGER = util.get_logger(__name__)
CORE_SCHEMA = {
    "$schema": "http://json-schema.org/schema#",
    "additionalProperties": False,
    "properties": {
        "mesos_master_url": {
            "description": "Mesos master URL. Must be set in format: "
                           "\"http://host:port\"",
            "format": "uri",
            "title": "Mesos Master URL",
            "type": "string"
        },
        "timeout": {
            "default": 5,
            "description": "Request timeout in seconds",
            "minimum": 1,
            "title": "Request timeout in seconds",
            "type": "integer"
        },
        "ssl_verify": {
            "type": "string",
            "default": "false",
            "title": "SSL Verification",
            "description": "Whether to verify SSL certs for HTTPS "
                           "or path to certs"
        },
        "pagination": {
            "type": "boolean",
            "default": True,
            "title": "Pagination",
            "description": "Whether to paginate output"
        }
    },
    "type": "object"
}


def _get_path(toml_config, path):
    """
    Retrieve the config at path

    :param config: Dict with the configuration values
    :type config: dict
    :param path: Path to the value. E.g. 'path.to.value'
    :type path: str
    :returns: Value stored at the given path
    :rtype: double, int, str, list or dict
    """

    for section in path.split('.'):
        toml_config = toml_config[section]

    return toml_config


def save(toml_config):
    """
    :param toml_config: TOML configuration object
    :type toml_config: MutableToml or Toml
    """

    serial = toml.dumps(toml_config.dictionary)
    path = get_config_path()

    util.ensure_file_exists(path)
    util.enforce_file_permissions(path)
    with util.open_file(path, 'w') as config_file:
        config_file.write(serial)


def _iterator(parent, dictionary):
    """
    Iterator for full-path key and values

    :param parent: Path to the value parameter
    :type parent: str
    :param dictionary: Value of the key
    :type dictionary: collection.Mapping
    :returns: An iterator of tuples for each property and value
    :rtype: iterator of (str, any) where any can be str, int, double, list
    """
    for key, value in dictionary.items():

        new_key = key
        if parent is not None:
            new_key = "{}.{}".format(parent, key)

        # If value is not a mapping, yield the value
        if not isinstance(value, collections.Mapping):
            yield (new_key, value)
        else:
            # Otherwise recursively call iterator
            for key_val_pair in _iterator(new_key, value):
                yield key_val_pair


class Toml(collections.Mapping):
    """Class for getting value from TOML.

    :param dictionary: configuration dictionary
    :type dictionary: collections.Mapping
    """

    def __init__(self, dictionary):
        self._dictionary = dictionary

    def __getitem__(self, path):
        """
        :param path: Path to the value. E.g. 'path.to.value'
        :type path: str
        :returns: Value stored at the given path
        :rtype: double, int, str, list or dict
        """

        toml_config = _get_path(self._dictionary, path)
        if isinstance(toml_config, collections.Mapping):
            return Toml(toml_config)
        else:
            return toml_config

    def __iter__(self):
        """
        :returns: Dictionary iterator
        :rtype: iterator
        """

        return iter(self._dictionary)

    def property_items(self):
        """Iterator for full-path keys and values

        :returns: Iterator for pull-path keys and values
        :rtype: iterator of tuples
        """

        return _iterator(None, self._dictionary)

    def __len__(self):
        """
        :returns: The length of the dictionary
        :rtype: int
        """

        return len(self._dictionary)


class MutableToml(collections.MutableMapping):
    """Class for managing CLI configuration through TOML.

    :param dictionary: configuration dictionary
    :type dictionary: collections.MutableMapping
    """

    def __init__(self, dictionary):
        self._dictionary = dictionary

    @property
    def dictionary(self):
        """
        :return: configuration dictionary
        :rtype: collections.MutableMapping
        """
        return self._dictionary

    def __getitem__(self, path):
        """
        :param path: Path to the value. E.g. 'path.to.value'
        :type path: str
        :returns: Value stored at the given path
        :rtype: double, int, str, list or dict
        """

        toml_config = _get_path(self._dictionary, path)

        # If config is a mutable mapping, return a MutableMapping object
        if isinstance(toml_config, collections.MutableMapping):
            return MutableToml(toml_config)

        # Otherwise return the config value
        return toml_config

    def __iter__(self):
        """
        :returns: Dictionary iterator
        :rtype: iterator
        """

        return iter(self._dictionary)

    def property_items(self):
        """Iterator for full-path keys and values

        :returns: Iterator for pull-path keys and values
        :rtype: iterator of tuples
        """

        return _iterator(None, self._dictionary)

    def __len__(self):
        """
        :returns: The length of the dictionary
        :rtype: int
        """

        return len(self._dictionary)

    def __setitem__(self, path, value):
        """
        :param path: Path to set
        :type path: str
        :param value: Value to store
        :type value: double, int, str, list or dict
        """

        toml_config = self._dictionary

        sections = path.split('.')
        for section in sections[:-1]:
            toml_config = toml_config.setdefault(section, {})

        toml_config[sections[-1]] = value

    def __delitem__(self, path):
        """
        :param path: Path to delete
        :type path: str
        """
        toml_config = self._dictionary

        sections = path.split('.')
        for section in sections[:-1]:
            toml_config = toml_config[section]

        del toml_config[sections[-1]]


def get_config_path():
    """ Returns the path to the MESOS config file.

    :returns: path to the MESOS config file
    :rtype: str
    """

    return os.environ.get(constants.MESOS_CONFIG_ENV, get_default_config_path())


def get_default_config_path():
    """Returns the default path to the MESOS config file.

    :returns: path to the MESOS config file
    :rtype: str
    """

    return os.path.expanduser(
        os.path.join("~",
                     constants.MESOS_DIR,
                     'mesos.toml'))


def get_config(mutable=False):
    """Returns the MESOS configuration object and creates config file is None
    found and `MESOS_CONFIG` set to default value. Only use to get the config,
    not to resolve a specific config parameter. This should be done with
    `get_config_val`.

    :param mutable: True if the returned Toml object should be mutable
    :type mutable: boolean
    :returns: Configuration object
    :rtype: Toml | MutableToml
    """

    path = get_config_path()
    default = get_default_config_path()

    if path == default:
        util.ensure_dir_exists(os.path.dirname(default))
    return load_from_path(path, mutable)


def load_from_path(path, mutable=False):
    """Loads a TOML file from the path

    :param path: Path to the TOML file
    :type path: str
    :param mutable: True if the returned Toml object should be mutable
    :type mutable: boolean
    :returns: Map for the configuration file
    :rtype: Toml | MutableToml
    """

    util.ensure_file_exists(path)
    util.enforce_file_permissions(path)
    with util.open_file(path, 'r') as config_file:
        try:
            toml_obj = toml.loads(config_file.read())
        except Exception as err:
            raise MesosException(
                'Error parsing config file at [{}]: {}'.format(path, err))
        return (MutableToml if mutable else Toml)(toml_obj)


def split_key(name):
    """
    :param name: the full property path - e.g. marathon.url
    :type name: str
    :returns: the section and property name
    :rtype: (str, str)
    """

    terms = name.split('.', 1)
    if len(terms) != 2:
        raise MesosException('Property name must have both a section and '
                             'key: <section>.<key> - E.g. marathon.url')

    return terms[0], terms[1]


def get_config_val(name, config=None):
    """Returns the config value for the specified key. Looks for corresponding
    environment variable first, and if it doesn't exist, uses the config value.
    - "core" properties get resolved to env variable MESOS_SUBKEY. With the
    exception of subkeys that already start with MESOS, in which case we look
    for SUBKEY first, and "MESOS_SUBKEY" second, and finally the config value.
    - everything else gets resolved to MESOS_SECTION_SUBKEY

    :param name: name of paramater
    :type name: str
    :param config: config
    :type config: Toml
    :returns: value of 'name' parameter
    :rtype: str | None
    """

    if config is None:
        config = get_config()

    section, subkey = split_key(name.upper())

    if section == "CORE":
        if subkey.startswith("MESOS") and os.environ.get(subkey):
            env_var = subkey
        else:
            env_var = "MESOS_{}".format(subkey)
    else:
        env_var = "MESOS_{}_{}".format(section, subkey)

    return os.environ.get(env_var) or config.get(name)


def get_config_schema(command):
    """
    :param command: the subcommand name
    :type command: str
    :returns: the subcommand's configuration schema
    :rtype: dict
    """

    # core.* config variables are special.  They're valid, but don't
    # correspond to any particular subcommand, so we must handle them
    # separately.
    if command == "core":
        return CORE_SCHEMA

    raise NotImplementedError("Schema for command '{cmd}' not implemented"
                              .format(cmd=command))


def check_config(toml_config_pre, toml_config_post, section):
    """
    :param toml_config_pre: dictionary for the value before change
    :type toml_config_pre: Toml
    :param toml_config_post: dictionary for the value with change
    :type toml_config_post: Toml
    :param section: section of the config to check
    :type section: str
    :returns: process status
    :rtype: int
    """

    errors_pre = util.validate_json(toml_config_pre.dictionary[section],
                                    get_config_schema(section))
    errors_post = util.validate_json(toml_config_post.dictionary[section],
                                     get_config_schema(section))

    LOGGER.info('Comparing changes in the configuration...')
    LOGGER.info('Errors before the config command: %r', errors_pre)
    LOGGER.info('Errors after the config command: %r', errors_post)

    if len(errors_post) != 0:
        if len(errors_pre) == 0:
            raise MesosException(util.list_to_err(errors_post))

        def _errs(errs):
            return set([e.split('\n')[0] for e in errs])

        diff_errors = _errs(errors_post) - _errs(errors_pre)
        if len(diff_errors) != 0:
            raise MesosException(util.list_to_err(errors_post))


def set_val(name, value):
    """
    :param name: name of paramater
    :type name: str
    :param value: value to set to paramater `name`
    :type param: str
    :returns: Toml config, message of change
    :rtype: Toml, str
    """

    toml_config = get_config(True)

    section, subkey = split_key(name)

    config_schema = get_config_schema(section)

    new_value = jsonitem.parse_json_value(subkey, value, config_schema)

    toml_config_pre = copy.deepcopy(toml_config)
    if section not in toml_config_pre.dictionary:
        toml_config_pre.dictionary[section] = {}

    value_exists = name in toml_config
    old_value = toml_config.get(name)

    toml_config[name] = new_value

    check_config(toml_config_pre, toml_config, section)

    save(toml_config)

    msg = "[{}]: ".format(name)
    if not value_exists:
        msg += "set to '{}'".format(new_value)
    elif old_value == new_value:
        msg += "already set to '{}'".format(old_value)
    else:
        msg += "changed from '{}' to '{}'".format(old_value, new_value)

    return toml_config, msg


def _generate_choice_msg(name, value):
    """
    :param name: name of the property
    :type name: str
    :param value: dictionary for the value
    :type value: Toml
    :returns: an error message for top level properties
    :rtype: str
    """

    message = ("Property {!r} doesn't fully specify a value - "
               "possible properties are:").format(name)
    for key, _ in sorted(value.property_items()):
        message += '\n{}.{}'.format(name, key)

    return message


def unset(name):
    """
    :param name: name of config value to unset
    :type name: str
    :returns: message of property removed
    :rtype: str
    """

    toml_config = get_config(True)
    toml_config_pre = copy.deepcopy(toml_config)
    section = name.split(".", 1)[0]
    if section not in toml_config_pre.dictionary:
        toml_config_pre.dictionary[section] = {}
    value = toml_config.pop(name, None)

    if value is None:
        raise MesosException("Property {!r} doesn't exist".format(name))
    elif isinstance(value, collections.Mapping):
        raise MesosException(_generate_choice_msg(name, Toml(value)))
    else:
        msg = "Removed [{}]".format(name)
        save(toml_config)
        return msg


def generate_choice_msg(name, value):
    """
    :param name: name of the property
    :type name: str
    :param value: dictionary for the value
    :type value: dcos.config.Toml
    :returns: an error message for top level properties
    :rtype: str
    """

    message = ("Property {!r} doesn't fully specify a value - "
               "possible properties are:").format(name)
    for key, _ in sorted(value.property_items()):
        message += '\n{}.{}'.format(name, key)

    return message
