# !/usr/bin/env python
# encoding=utf-8

import time

from rpc.auth.ttypes import Credential
from rpc.auth.ttypes import UserType
from emr.client.clientfactory import ClientFactory
from emr.service.ttypes import CreateClusterRequest, TerminateClusterRequest
from emr.service.ttypes import AddInstanceGroupRequest
from emr.service.ttypes import InstanceGroupRole
from emr.service.ttypes import StateCode

'''
 Copyright 2015, Xiaomi.
 All rights reserved.
 Author: liupengcheng@xiaomi.com
'''

app_key = ""  # replace with your app id
app_secret = ""  # replace with your app secret
# create client
client_factory = ClientFactory(Credential(UserType.APP_SECRET,
  app_key, app_secret))
emr_client = client_factory.new_emr_service_client()

# ---------------------- create cluster -----------------------
# Note: create cluster may take several minutes, usually 4-6 minutes
create_cluster_req_full_args = CreateClusterRequest(
  "cluster1",autoTerminate=False, terminationProtected=True, region="ec2.cn-north-1",
  purpose="emr", keyPair="keypair1")
add_master_group_req = AddInstanceGroupRequest("masterGroup")
add_master_group_req.role = InstanceGroupRole.MASTER
add_master_group_req.instanceType = "master.test"
add_master_group_req.requestedInstanceCount = 1

add_control_group_req = AddInstanceGroupRequest("controlGroup")
add_control_group_req.role = InstanceGroupRole.CONTROL
add_control_group_req.instanceType = "control.test"
add_control_group_req.requestedInstanceCount = 3

add_core_group_req = AddInstanceGroupRequest("coreGroup")
add_core_group_req.role = InstanceGroupRole.CORE
add_core_group_req.instanceType = "core.test"
add_core_group_req.requestedInstanceCount = 1

add_task_group_req = AddInstanceGroupRequest("taskGroup")
add_task_group_req.role = InstanceGroupRole.TASK
add_task_group_req.instanceType = "task.test"
add_task_group_req.requestedInstanceCount = 1
create_cluster_req_full_args.addInstanceGroupRequests = [
  add_master_group_req,
  add_control_group_req,
  add_core_group_req,
  add_task_group_req]

create_cluster_resp_full_args = emr_client.createCluster(
  create_cluster_req_full_args)
create_cluster_timeout = 5 * 60
polling_start_time = time.time()
while True:
  time.sleep(5)
  cluster_detail = emr_client.describeCluster(create_cluster_resp_full_args.clusterId)
  if cluster_detail.clusterStatus.state == StateCode.C_RUNNING:
    print "Cluster is running."
    print "cluster detail:\n" + str(cluster_detail)
    break
  if time.time() - polling_start_time > create_cluster_timeout:
    raise Exception("Create cluster error: polling exceeded max timeout")

# ---------------------- describe cluster -----------------------------------
cluster_detail = emr_client.describeCluster(
  create_cluster_resp_full_args.clusterId)
if cluster_detail:
  print "cluster detail:\n" + str(cluster_detail)
else:
  print "cluster detail response is None"

# ---------------------- list clusters --------------------------------------
time_stop = time.time()
time_start = time_stop - 10 * 3600 # from ten minutes ago
list_clusters = emr_client.listClusters(time_start, time_stop)
if list_clusters:
  print "list_clusters:\n" + str(list_clusters)
else:
  print "list_clusters response is None"
# ----------------------- describe instanceGroup ----------------------------
instance_group_detail = emr_client.describeInstanceGroup(
  create_cluster_resp_full_args.addInstanceGroupResponses[0].instanceGroupId)
if instance_group_detail:
  print "instanceGroup detail:\n" + str(instance_group_detail)
else:
  print "instanceGroup detail response is None"
# ----------------------- list instanceGroups --------------------------------
list_instanceGroups = emr_client.listInstanceGroups(
  create_cluster_resp_full_args.clusterId)
if list_instanceGroups:
  print "list_instanceGroups:\n" + str(list_instanceGroups)
else:
  print "list_instanceGroups response is None"
# ------------------------ list instances in cluster -------------------------
list_instances_in_cluster = emr_client.listInstancesInCluster(
  create_cluster_resp_full_args.clusterId)
if list_instances_in_cluster:
  print "list_instances_in_cluster\n" + str(list_instances_in_cluster)
else:
  print "list_instances_in_cluster response is None"
# -----------------------list instances in instanceGroup --------------------
list_instances_in_group = emr_client.listInstancesInGroup(
  create_cluster_resp_full_args.clusterId,
  create_cluster_resp_full_args.addInstanceGroupResponses[0].instanceGroupId,
  InstanceGroupRole.MASTER)
if list_instances_in_group:
  print "list_instance_in_group\n" + str(list_instances_in_group)
else:
  print "list_instances_in_cluster response is None"

# ----------------------- terminate cluster ----------------------------------
# Note: terminate cluste also take several minutes, usually 3-4 minutes
terminate_cluster_req = TerminateClusterRequest(create_cluster_resp_full_args.clusterId)
terminate_cluster_resp = emr_client.terminateCluster(terminate_cluster_req)
if terminate_cluster_resp:
  print "terminate cluster response:" + terminate_cluster_resp
else:
  print "terminate cluster response is None"