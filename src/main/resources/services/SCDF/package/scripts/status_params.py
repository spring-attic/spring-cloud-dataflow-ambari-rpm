#!/usr/bin/env python
"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

"""

from resource_management import *

config = Script.get_config()
pid_dir = config['configurations']['scdf-server-env']['scdf_pid_dir']
server_pid_file = format("{pid_dir}/scdf_server.pid")
h2_pid_file = format("{pid_dir}/scdf_h2.pid")
collector_pid_file = format("{pid_dir}/scdf_collector.pid")
data_dir = config['configurations']['scdf-server-env']['scdf_data_dir']
