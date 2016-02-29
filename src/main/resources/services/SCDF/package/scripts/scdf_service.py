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

from resource_management import *

def scdf_service(action='none', name='none'):
  import params
  import status_params

  pid_file = format("{pid_dir}/scdf_{name}.pid")
  no_op_test = format("ls {pid_file} >/dev/null 2>&1 && ps `cat {pid_file}` >/dev/null 2>&1")

  if name == 'server':
    process_grep = "grep spring-cloud-dataflow-server-yarn | grep -v spring-cloud-dataflow-server-yarn-h2"
  elif name == 'h2':
    process_grep = "grep spring-cloud-dataflow-server-yarn-h2"

  find_proc = format("{jps_binary} -l  | {process_grep}")
  write_pid = format("{find_proc} | awk {{'print $1'}} > {pid_file}")
  crt_pid_cmd = format("{find_proc} && {write_pid}")

  if action == 'start':
    if name == 'server':
      process_cmd = format("source {conf_dir}/scdf-server-env.sh ; /opt/pivotal/dataflow/bin/dataflow-server-yarn > {log_dir}/server.out 2>&1")
    elif name == 'h2':
      process_cmd = format("source {conf_dir}/scdf-server-env.sh ; /opt/pivotal/dataflow/bin/dataflow-server-yarn-h2 --dataflow.database.h2.directory={data_dir} --dataflow.database.h2.port={h2_port} > {log_dir}/h2.out 2>&1")

    Execute(process_cmd,
           user=params.scdf_user,
           wait_for_finish=False
    )

    Execute(crt_pid_cmd,
            user=params.scdf_user,
            logoutput=True,
            tries=6,
            try_sleep=10
    )

  elif action == 'stop':
    process_dont_exist = format("! ({no_op_test})")
    pid = format("`cat {pid_file}` >/dev/null 2>&1")
    Execute(format("kill {pid}"),
            not_if=process_dont_exist
    )
    Execute(format("kill -9 {pid}"),
            not_if=format("sleep 2; {process_dont_exist} || sleep 20; {process_dont_exist}"),
            ignore_failures=True
    )
    Execute(format("rm -f {pid_file}"))

