#!/bin/bash

python <<EOT
import json, os, socket
import xml.etree.ElementTree as ET
from pprint import pprint

def updateRepoWithScdf(repoinfoxml):
  scdf_repo='SCDF-@version@'
  scdf_repo_str = '<repo><baseurl>http://repo.spring.io/@springYumRepoId@/scdf/@version@</baseurl><repoid>' + scdf_repo + '</repoid><reponame>' + scdf_repo + '</reponame></repo>'
  is_scdfrepo_set = None

  tree = ET.parse(repoinfoxml)
  root = tree.getroot()

  for os_tag in root.findall('.//os'):
    if ('type' in os_tag.attrib and os_tag.attrib['type'] == 'redhat6') or ('family' in os_tag.attrib and os_tag.attrib['family'] == 'redhat6') or ('type' in os_tag.attrib and os_tag.attrib['type'] == 'redhat7') or ('family' in os_tag.attrib and os_tag.attrib['family'] == 'redhat7'):
      for reponame in os_tag.findall('.//reponame'):
        if 'SCDF-@version@' in reponame.text:
          is_scdfrepo_set = True
      if is_scdfrepo_set is None:
        scdf_element = ET.fromstring(scdf_repo_str)
        os_tag.append(scdf_element)
  if is_scdfrepo_set is None:
    tree.write(repoinfoxml)

if os.path.exists('/var/lib/ambari-server/resources/stacks/PHD/3.0/role_command_order.json'):
  json_data=open('/var/lib/ambari-server/resources/stacks/PHD/3.0/role_command_order.json', 'r+')
  data = json.load(json_data)
  data['general_deps']['SCDF-INSTALL'] = ['HDFS-INSTALL']
  data['general_deps']['SCDFSERVER-START'] = ['SCDFH2-START','ZOOKEEPER_SERVER-START','NODEMANAGER-START','RESOURCEMANAGER-START']
  json_data.seek(0)
  json.dump(data, json_data, indent=2)
  json_data.close()

if os.path.exists('/var/lib/ambari-server/resources/stacks/HDP/2.2/role_command_order.json'):
  json_data=open('/var/lib/ambari-server/resources/stacks/HDP/2.2/role_command_order.json', 'r+')
  data = json.load(json_data)
  data['general_deps']['SCDF-INSTALL'] = ['HDFS-INSTALL']
  data['general_deps']['SCDFSERVER-START'] = ['SCDFH2-START','ZOOKEEPER_SERVER-START','KAFKA_BROKER-START','NODEMANAGER-START','RESOURCEMANAGER-START']
  json_data.seek(0)
  json.dump(data, json_data, indent=2)
  json_data.close()

if os.path.exists('/var/lib/ambari-server/resources/stacks/HDP/2.3/role_command_order.json'):
  json_data=open('/var/lib/ambari-server/resources/stacks/HDP/2.3/role_command_order.json', 'r+')
  data = json.load(json_data)
  data['general_deps']['SCDF-INSTALL'] = ['HDFS-INSTALL']
  data['general_deps']['SCDFSERVER-START'] = ['SCDFH2-START','ZOOKEEPER_SERVER-START','KAFKA_BROKER-START','NODEMANAGER-START','RESOURCEMANAGER-START']
  json_data.seek(0)
  json.dump(data, json_data, indent=2)
  json_data.close()

if os.path.exists('/var/lib/ambari-server/resources/stacks/HDP/2.4/role_command_order.json'):
  json_data=open('/var/lib/ambari-server/resources/stacks/HDP/2.4/role_command_order.json', 'r+')
  data = json.load(json_data)
  data['general_deps']['SCDF-INSTALL'] = ['HDFS-INSTALL']
  data['general_deps']['SCDFSERVER-START'] = ['SCDFH2-START','ZOOKEEPER_SERVER-START','KAFKA_BROKER-START','NODEMANAGER-START','RESOURCEMANAGER-START']
  json_data.seek(0)
  json.dump(data, json_data, indent=2)
  json_data.close()

if os.path.exists('/var/lib/ambari-server/resources/stacks/HDP/2.5/role_command_order.json'):
  json_data=open('/var/lib/ambari-server/resources/stacks/HDP/2.5/role_command_order.json', 'r+')
  data = json.load(json_data)
  data['general_deps']['SCDF-INSTALL'] = ['HDFS-INSTALL']
  data['general_deps']['SCDFSERVER-START'] = ['SCDFH2-START','ZOOKEEPER_SERVER-START','KAFKA_BROKER-START','NODEMANAGER-START','RESOURCEMANAGER-START']
  json_data.seek(0)
  json.dump(data, json_data, indent=2)
  json_data.close()

if os.path.exists('/var/lib/ambari-server/resources/stacks/HDP/2.6/role_command_order.json'):
  json_data=open('/var/lib/ambari-server/resources/stacks/HDP/2.6/role_command_order.json', 'r+')
  data = json.load(json_data)
  data['general_deps']['SCDF-INSTALL'] = ['HDFS-INSTALL']
  data['general_deps']['SCDFSERVER-START'] = ['SCDFH2-START','ZOOKEEPER_SERVER-START','KAFKA_BROKER-START','NODEMANAGER-START','RESOURCEMANAGER-START']
  json_data.seek(0)
  json.dump(data, json_data, indent=2)
  json_data.close()

if os.path.exists('/var/lib/ambari-server/resources/stacks/PHD/3.0/repos/repoinfo.xml'):
  updateRepoWithScdf('/var/lib/ambari-server/resources/stacks/PHD/3.0/repos/repoinfo.xml')

if os.path.exists('/var/lib/ambari-server/resources/stacks/HDP/2.2/repos/repoinfo.xml'):
  updateRepoWithScdf('/var/lib/ambari-server/resources/stacks/HDP/2.2/repos/repoinfo.xml')

if os.path.exists('/var/lib/ambari-server/resources/stacks/HDP/2.3/repos/repoinfo.xml'):
  updateRepoWithScdf('/var/lib/ambari-server/resources/stacks/HDP/2.3/repos/repoinfo.xml')

if os.path.exists('/var/lib/ambari-server/resources/stacks/HDP/2.4/repos/repoinfo.xml'):
  updateRepoWithScdf('/var/lib/ambari-server/resources/stacks/HDP/2.4/repos/repoinfo.xml')

if os.path.exists('/var/lib/ambari-server/resources/stacks/HDP/2.5/repos/repoinfo.xml'):
  updateRepoWithScdf('/var/lib/ambari-server/resources/stacks/HDP/2.5/repos/repoinfo.xml')

if os.path.exists('/var/lib/ambari-server/resources/stacks/HDP/2.6/repos/repoinfo.xml'):
  updateRepoWithScdf('/var/lib/ambari-server/resources/stacks/HDP/2.6/repos/repoinfo.xml')

EOT


