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
from resource_management.libraries.functions.version import compare_versions
from resource_management import *
from compat import get_full_stack_version
import status_params

# server configurations
config = Script.get_config()
tmp_dir = Script.get_tmp_dir()
stack_version = str(config['hostLevelParams']['stack_version'])
stack_version = get_full_stack_version(stack_version)
stack_name = str(config['hostLevelParams']['stack_name']).lower()
conf_dir = "/etc/scdf/conf"

# common configs
log_dir = config['configurations']['scdf-server-env']['scdf_log_dir']
user_group = config['configurations']['cluster-env']['user_group']
pid_dir = status_params.pid_dir
data_dir = status_params.data_dir
java64_home = config['hostLevelParams']['java_home']
jps_binary = format("{java64_home}/bin/jps")
deployer_dir = config['configurations']['scdf-site']['deployer.yarn.app.baseDir'].strip()

if len(deployer_dir)<1:
  deployer_dir = "/dataflow"

# scdf configs
h2_port = config['configurations']['scdf-site']['h2.server.port']
server_port = config['configurations']['scdf-site']['dataflow.server.port']
spring_cloud_stream_kafka_binder_brokers = config['configurations']['scdf-site']['spring.cloud.stream.kafka.binder.brokers'].strip()
spring_cloud_stream_kafka_binder_zknodes = config['configurations']['scdf-site']['spring.cloud.stream.kafka.binder.zkNodes'].strip()
spring_rabbitmq_addresses = config['configurations']['scdf-site']['spring.rabbitmq.addresses'].strip()
spring_rabbitmq_username = config['configurations']['scdf-site']['spring.rabbitmq.username'].strip()
spring_rabbitmq_password = config['configurations']['scdf-site']['spring.rabbitmq.password'].strip()

if stack_name == 'phd':
  hadoop_distro = "phd30"
elif stack_name == 'hdp':
  hadoop_distro = "hdp22"
else:
  hadoop_distro = None

# scdf env configs
scdf_server_env_sh_template = config['configurations']['scdf-server-env']['content']
scdf_shell_env_sh_template = config['configurations']['scdf-shell-env']['content']
scdf_user = config['configurations']['scdf-server-env']['scdf_user']
scdf_hdfs_user_dir = format("/user/{scdf_user}")

# hdfs ha
dfs_ha_enabled = False
dfs_ha_nameservices = default("/configurations/hdfs-site/dfs.nameservices", None)
dfs_ha_namenode_ids = default(format("/configurations/hdfs-site/dfs.ha.namenodes.{dfs_ha_nameservices}"), None)
dfs_ha_automatic_failover_enabled = default("/configurations/hdfs-site/dfs.ha.automatic-failover.enabled", False)
dfs_ha_namemodes_ids_list = []

if dfs_ha_namenode_ids:
  dfs_ha_namemodes_ids_list = dfs_ha_namenode_ids.split(",")
  dfs_ha_namenode_ids_array_len = len(dfs_ha_namemodes_ids_list)
  if dfs_ha_namenode_ids_array_len > 1:
    dfs_ha_enabled = True

# cluster configs


zk_port = config['configurations']['zoo.cfg']['clientPort']
fs_defaultfs = config['configurations']['core-site']['fs.defaultFS']

if (('yarn-site' in config['configurations']) and ('mapred-site' in config['configurations'])):
  yarn_installed = True
  yarn_rm_address = config['configurations']['yarn-site']['yarn.resourcemanager.address']
  yarn_rm_address_host = yarn_rm_address.split(':')[0]
  yarn_rm_address_port = yarn_rm_address.split(':')[1]
  yarn_rm_scheduler_address = config['configurations']['yarn-site']['yarn.resourcemanager.scheduler.address']
  job_history_address = config['configurations']['mapred-site']['mapreduce.jobhistory.address']
  yarn_app_classpath = config['configurations']['yarn-site']['yarn.application.classpath']
  yarn_app_classpath = yarn_app_classpath.replace('${hdp.version}', stack_version)
  yarn_app_classpath = yarn_app_classpath.replace('${stack.version}', stack_version)
  yarn_app_classpath = yarn_app_classpath.replace('${stack.name}', stack_name)
  mr_app_classpath = config['configurations']['mapred-site']['mapreduce.application.classpath']
  mr_app_classpath = mr_app_classpath.replace('${hdp.version}', stack_version)
  mr_app_classpath = mr_app_classpath.replace('${stack.version}', stack_version)
  mr_app_classpath = mr_app_classpath.replace('${stack.name}', stack_name)
else:
  yarn_installed = False

# collect what is actually installed
if 'kafka_broker_hosts' in config['clusterHostInfo'] and \
    len(config['clusterHostInfo']['kafka_broker_hosts'])>0:
  kafka_installed = True
  kafka_broker_hosts = config['clusterHostInfo']['kafka_broker_hosts']
  kafka_broker_hosts.sort()
  kafka_broker_hosts_strings = []

  # kafka eiher in port or listener address
  if ('port' in config['configurations']['kafka-broker']):
    kafka_port = config['configurations']['kafka-broker']['port']
  else:
    kafka_port = config['configurations']['kafka-broker']['listeners'].split(':')[-1]

  for kafka_broker_host in kafka_broker_hosts:
    kafka_broker_hosts_strings.append(format("{kafka_broker_host}:{kafka_port}"))
  kafka_broker_connect = ','.join(kafka_broker_hosts_strings)
else:
  kafka_installed = False

if 'zookeeper_hosts' in config['clusterHostInfo'] and \
    len(config['clusterHostInfo']['zookeeper_hosts'])>0:
  zk_installed = True
  zookeeper_hosts = config['clusterHostInfo']['zookeeper_hosts']
  zookeeper_hosts.sort()
  zk_connects_strings = []
  for zk_host in zookeeper_hosts:
    zk_connects_strings.append(zk_host+":"+str(zk_port))
  zk_connect = ','.join(zk_connects_strings)
else:
  zk_installed = False

if 'scdfh2_hosts' in config['clusterHostInfo'] and \
    len(config['clusterHostInfo']['scdfh2_hosts'])>0:
  h2_installed = True
  h2_server = config['clusterHostInfo']['scdfh2_hosts'][0]
else:
  h2_installed = False

if 'scdfserver_hosts' in config['clusterHostInfo'] and \
    len(config['clusterHostInfo']['scdfserver_hosts'])>0:
  scdf_server_installed = True
  scdf_server = config['clusterHostInfo']['scdfserver_hosts'][0]
else:
  scdf_server_installed = False

rabbitmq_installed = False

# use kafka from cluster if it's not overridden
if kafka_installed and len(spring_cloud_stream_kafka_binder_brokers)<1:
  spring_cloud_stream_kafka_binder_brokers = kafka_broker_connect

# we don't have rabbit in cluster so mark it installed if address is given
if len(spring_rabbitmq_addresses)>1:
  rabbitmq_installed = True

# kafka zk settings
if len(spring_cloud_stream_kafka_binder_zknodes)<1 and zk_installed:
  spring_cloud_stream_kafka_binder_zknodes = zk_connect

# for dataflow shell
scdf_shell_hdfs_address = fs_defaultfs
scdf_shell_server_address = format("http://{scdf_server}:{server_port}")

# needed for hdfs directory setup
hadoop_conf_dir = "/etc/hadoop/conf"
hdfs_user = config['configurations']['hadoop-env']['hdfs_user']
security_enabled = config['configurations']['cluster-env']['security_enabled']
hdfs_user_keytab = config['configurations']['hadoop-env']['hdfs_user_keytab']
hdfs_principal_name = config['configurations']['hadoop-env']['hdfs_principal_name']

# compat tweak for ambari 2.0.0
try:
  kinit_path_local = functions.get_kinit_path(["/usr/bin", "/usr/kerberos/bin", "/usr/sbin"])
except TypeError:
  kinit_path_local = functions.get_kinit_path()

hadoop_bin_dir = "/usr/hdp/current/hadoop-client/bin"
hdfs_site = config['configurations']['hdfs-site']
default_fs = config['configurations']['core-site']['fs.defaultFS']
action_create_delayed = "create_delayed"
action_create = "create"

# for other sec
if security_enabled:
  nn_principal_name = config['configurations']['hdfs-site']['dfs.namenode.kerberos.principal']
  rm_principal_name = config['configurations']['yarn-site']['yarn.resourcemanager.principal']
  jhs_principal_name = config['configurations']['mapred-site']['mapreduce.jobhistory.principal']
  user_principal_name = config['configurations']['scdf-site']['spring.hadoop.security.userPrincipal']
  user_keytab = config['configurations']['scdf-site']['spring.hadoop.security.userKeytab']
  scdf_kafka_keytab_path = user_keytab
  _hostname_lowercase = config['hostname'].lower()
  scdf_kafka_bare_jaas_principal = 'kafka'
  scdf_kafka_jaas_principal = user_principal_name.replace('_HOST',_hostname_lowercase)
else:
  scdf_kafka_keytab_path = None
  scdf_kafka_bare_jaas_principal = None
  scdf_kafka_jaas_principal = None



import functools
try:
  HdfsDirectory = functools.partial(
    HdfsDirectory,
    conf_dir=hadoop_conf_dir,
    hdfs_user=hdfs_user,
    security_enabled = security_enabled,
    keytab = hdfs_user_keytab,
    kinit_path_local = kinit_path_local,
    bin_dir = hadoop_bin_dir
  )
except NameError:
  HdfsDirectory = functools.partial(
    HdfsResource,
    type="directory",
    user=hdfs_user,
    security_enabled = security_enabled,
    keytab = hdfs_user_keytab,
    kinit_path_local = kinit_path_local,
    hadoop_bin_dir = hadoop_bin_dir,
    hadoop_conf_dir = hadoop_conf_dir,
    principal_name = hdfs_principal_name,
    hdfs_site = hdfs_site,
    default_fs = default_fs
  )
  action_create_delayed = "create_on_execute"
  action_create = "execute"

