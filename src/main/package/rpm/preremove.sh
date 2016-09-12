#!/bin/bash

python <<EOT
import json, os, socket
import xml.etree.ElementTree as ET
from pprint import pprint

def updateRepoWithScdf(repoinfoxml):
  is_scdfrepo_set = None

  tree = ET.parse(repoinfoxml)
  root = tree.getroot()

  for os_tag in root.findall('.//os'):
    if ('type' in os_tag.attrib and os_tag.attrib['type'] == 'redhat6') or ('family' in os_tag.attrib and os_tag.attrib['family'] == 'redhat6') or ('type' in os_tag.attrib and os_tag.attrib['type'] == 'redhat7') or ('family' in os_tag.attrib and os_tag.attrib['family'] == 'redhat7'):
      for repo in os_tag.findall('.//repo'):
        for reponame in repo.findall('.//reponame'):
          if 'SCDF-1.0' in reponame.text:
            is_scdfrepo_set = True
            os_tag.remove(repo)
  if is_scdfrepo_set == True:
    tree.write(repoinfoxml)

if os.path.exists('/var/lib/ambari-server/resources/stacks/PHD/3.0/role_command_order.json'):
  json_data=open('/var/lib/ambari-server/resources/stacks/PHD/3.0/role_command_order.json', 'r+')
  data = json.load(json_data)
  if data['general_deps'].has_key('SCDF-INSTALL'):
    data['general_deps'].pop('SCDF-INSTALL')
  if data['general_deps'].has_key('SCDFSERVER-START'):
    data['general_deps'].pop('SCDFSERVER-START')
  json_data.seek(0)
  json_data.truncate()
  json.dump(data, json_data, indent=2)
  json_data.close()

if os.path.exists('/var/lib/ambari-server/resources/stacks/HDP/2.2/role_command_order.json'):
  json_data=open('/var/lib/ambari-server/resources/stacks/HDP/2.2/role_command_order.json', 'r+')
  data = json.load(json_data)
  if data['general_deps'].has_key('SCDF-INSTALL'):
    data['general_deps'].pop('SCDF-INSTALL')
  if data['general_deps'].has_key('SCDFSERVER-START'):
    data['general_deps'].pop('SCDFSERVER-START')
  json_data.seek(0)
  json_data.truncate()
  json.dump(data, json_data, indent=2)
  json_data.close()

if os.path.exists('/var/lib/ambari-server/resources/stacks/HDP/2.3/role_command_order.json'):
  json_data=open('/var/lib/ambari-server/resources/stacks/HDP/2.3/role_command_order.json', 'r+')
  data = json.load(json_data)
  if data['general_deps'].has_key('SCDF-INSTALL'):
    data['general_deps'].pop('SCDF-INSTALL')
  if data['general_deps'].has_key('SCDFSERVER-START'):
    data['general_deps'].pop('SCDFSERVER-START')
  json_data.seek(0)
  json_data.truncate()
  json.dump(data, json_data, indent=2)
  json_data.close()

if os.path.exists('/var/lib/ambari-server/resources/stacks/HDP/2.4/role_command_order.json'):
  json_data=open('/var/lib/ambari-server/resources/stacks/HDP/2.4/role_command_order.json', 'r+')
  data = json.load(json_data)
  if data['general_deps'].has_key('SCDF-INSTALL'):
    data['general_deps'].pop('SCDF-INSTALL')
  if data['general_deps'].has_key('SCDFSERVER-START'):
    data['general_deps'].pop('SCDFSERVER-START')
  json_data.seek(0)
  json_data.truncate()
  json.dump(data, json_data, indent=2)
  json_data.close()

if os.path.exists('/var/lib/ambari-server/resources/stacks/HDP/2.5/role_command_order.json'):
  json_data=open('/var/lib/ambari-server/resources/stacks/HDP/2.5/role_command_order.json', 'r+')
  data = json.load(json_data)
  if data['general_deps'].has_key('SCDF-INSTALL'):
    data['general_deps'].pop('SCDF-INSTALL')
  if data['general_deps'].has_key('SCDFSERVER-START'):
    data['general_deps'].pop('SCDFSERVER-START')
  json_data.seek(0)
  json_data.truncate()
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

EOT

