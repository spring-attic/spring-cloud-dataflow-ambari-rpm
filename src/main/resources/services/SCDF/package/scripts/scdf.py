"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Ambari Agent

"""
import os

from resource_management import *
from resource_management.core.exceptions import Fail
from yaml_utils import escape_yaml_property
import sys


def scdf(name = None):
  import params

  if name == "server":
    params.HdfsDirectory(params.deployer_dir,
                         action=params.action_create_delayed,
                         owner=params.scdf_user,
                         mode=0777
    )
    params.HdfsDirectory(params.scdf_hdfs_user_dir,
                         action=params.action_create_delayed,
                         owner=params.scdf_user,
                         mode=0777
    )
    params.HdfsDirectory(None, action=params.action_create)

  try:
    Directory(params.log_dir,
              owner=params.scdf_user,
              group=params.user_group,
              mode=0775,
              recursive=True
    )
  except Fail:
    Directory(params.log_dir,
              owner=params.scdf_user,
              group=params.user_group,
              mode=0775,
              create_parents=True
    )

  try:
    Directory([params.pid_dir, params.data_dir, params.conf_dir],
              owner=params.scdf_user,
              group=params.user_group,
              recursive=True
    )
  except Fail:
    Directory([params.pid_dir, params.data_dir, params.conf_dir],
              owner=params.scdf_user,
              group=params.user_group,
              create_parents=True
    )

  dfs_ha_map = {}
  if params.dfs_ha_enabled:
    for nn_id in params.dfs_ha_namemodes_ids_list:
      nn_host = params.config['configurations']['hdfs-site'][format('dfs.namenode.rpc-address.{dfs_ha_nameservices}.{nn_id}')]
      dfs_ha_map[nn_id] = nn_host

  configurations = params.config['configurations']['scdf-site']
  sec_filtered_map = {}
  for key,value in configurations.iteritems():
    if "security" in value:
      sec_filtered_map[key] = value

  File(format("{conf_dir}/servers.yml"),
       content=Template("servers.yml.j2",
                        extra_imports=[escape_yaml_property],
                        dfs_ha_map = dfs_ha_map,
                        configurations = configurations),
       owner=params.scdf_user,
       group=params.user_group
  )

  File(format("{conf_dir}/collectors.yml"),
       content=Template("collectors.yml.j2",
                        extra_imports=[escape_yaml_property],
                        dfs_ha_map = dfs_ha_map,
                        configurations = configurations),
       owner=params.scdf_user,
       group=params.user_group
  )

  File(format("{conf_dir}/scdf_kafka_jaas.conf"),
       content=Template("scdf_kafka_jaas.conf.j2",
                        configurations = configurations),
       owner=params.scdf_user,
       group=params.user_group
  )

  File(format("{conf_dir}/scdf-shell.init"),
       content=Template("scdf-shell.init.j2",
                        dfs_ha_map = dfs_ha_map),
       owner=params.scdf_user,
       group=params.user_group
  )

  File(format("{conf_dir}/hadoop.properties"),
       content=Template("hadoop.properties.j2",
                        dfs_ha_map = dfs_ha_map,
                        sec_filtered_map = sec_filtered_map),
       owner=params.scdf_user,
       group=params.user_group
  )

  File(format("{conf_dir}/scdf-server-env.sh"),
    owner=params.scdf_user,
    content=InlineTemplate(params.scdf_server_env_sh_template)
  )

  File(format("{conf_dir}/scdf-shell-env.sh"),
    owner=params.scdf_user,
    content=InlineTemplate(params.scdf_shell_env_sh_template)
  )

