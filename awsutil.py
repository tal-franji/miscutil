#!/usr/bin/python
# Simple wrapper around AWS CLI interface to create EC2 instances etc.
# NO LICENCE , NO LIABILITIES
# this is FREE and you are allowed to copy, change, slice  it - do whatever you want
# this was written to Amazon Linux/CentOS
# You can find the latest version at:
# https://github.com/tal-franji/miscutil/blob/master/awsutil.py
__author__ = "tal.franji@gmail.com"


import json
import os
import re
import shlex
import subprocess
import sys


# global defaults for AWS commands
GlobalDefaults = {
  'region': 'eu-west-1',
   'ec2-username': 'ec2-user', # 'hadoop' for emr
   'ssh-tunnel-ports': [4040, 8080, 8020, 9090, 8890, 9026],
}

# used by this module to read the defaults
def getDefault(param):
    global GlobalDefaults
    return GlobalDefaults.get(param)

#
def setDefault(param, value):
    global GlobalDefaults
    GlobalDefaults[param] = value


def System(cmd):
  print 'Executing: ', cmd
  return os.system(cmd)


def OSys(cmd):
  print 'Executing: ', cmd
  args = shlex.split(cmd)
  p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
  out, err = p.communicate()
  if err:
        print "ERROR =============\n", err
  return out

# assume cmd_template conatains %(xxx)s in it and use python % to fit in the params dict()
def PSystem(cmd_template, params):
    cmd = re.sub(r"\s+", " ", cmd_template)
    cmd = cmd % params
    return OSys(cmd)

# add --key=value to the command based on params dict
# return parsed json result
def AwsSystem(cmd_head, params, useDefaultForNone=True):
    # ignore params where value is None
    effective_params = dict()
    for k,v in params.iteritems():
        if v is None:
            if useDefaultForNone:
                v = getDefault(k)
        if v is not None:
            effective_params[k] = v

    cmd = cmd_head + " " + " ".join(["--%s %s" % (k, v) for k,v in  effective_params.iteritems()])
    json_str = OSys(cmd)
    if not json_str:
        return None
    try:
        j = json.loads(json_str)
        return j
    except:
        print "ERROR - Bad json from cmd=", cmd
        print "response: ", json_str
    return None

# get EC2 instance id using meta-data
def SelfInstnanceId():
    if sys.platform == "win32" or sys.platform == "darwin":
        return None  # when running script on laptop
    return OSys("wget -q -O - http://instance-data/latest/meta-data/instance-id")

# get Public IP for the instance
def SelfInstancePubIp():
    instance_id = SelfInstnanceId()
    if not instance_id:
        return "127.0.0.1"
    return GetPublicIp(instance_id)

# find instance by tag
def InstanceOfTagValue(tag, value):
    for inst in IterInstances():
        if inst.tags_dict.get(tag) == value:
            return inst
    return None


def CreateInstanceIfNotExist(tags={},singelton_tag_name=None,singelton_tag_value=None, **kwargs):
    if singelton_tag_name:
        instance = InstanceOfTagValue(singelton_tag_name, singelton_tag_value)
        if instance:
            print "Instance already exists! for tag={} value={} instance={} ip={}".format(
                singelton_tag_name, singelton_tag_value, instance.id, instance.pub_ip)
            return
    if 'Name' not in tags:
        tags['Name'] = singelton_tag_value
    tags[singelton_tag_name] = singelton_tag_value
    CreateInstance(tags=tags, **kwargs)

def UnderscoreToHyphen(key_val):
    res = {}
    for k,v in key_val.iteritems():
        k2 = k.replace("_", "-")
        res[k2] = v
    return res

def CreateInstance(tags={}, **kwargs):
    block_device_mapping = None
    snapshot_volume = kwargs.get('snapshot_volume')
    if snapshot_volume:
        tmpjson = "/tmp/tmp.json"
        with open(tmpjson, "w+t") as f:
            f.write("""[
                  {
                    "DeviceName": "/dev/sdh",
                    "Ebs": {
                    "SnapshotId": "%s",
                    "DeleteOnTermination": true
                    }
                  }
                ]
            """ % snapshot_volume)
        block_device_mapping = "file://" + tmpjson
    key_name = kwargs.get('key_name')
    if not key_name:
        print "ERROR - missing key-pair"
        return
    args = UnderscoreToHyphen(kwargs)
    params = {
              'region': None,
              'block-device-mappings': block_device_mapping,
              }
    params.update(args)
    j = AwsSystem("aws ec2 run-instances", params)
    instance_id = j.get("Instances", [{}])[0].get("InstanceId", None)
    if not instance_id:
        print "ERROR - bad response", str(j)
        return
    # looping over tags
    # TODO(franji): there shoudlbe a way to add multiple tags in single command
    # it does not work as documented - should try json parameter
    cmd = 'aws ec2 create-tags'
    params = {'resources': instance_id,'region': None}
    for k, v in tags.iteritems():
        params['tags'] = '"Key=%s,Value=%s"' % (k,v)
        AwsSystem(cmd, params)

    #tags_flag =  " ".join(['--tags "Key=%s,Value=%s"' % (k,v) for k, v in tags.iteritems()])
    #cmd = 'aws ec2 create-tags ' + tags_flag
    #AwsSystem(cmd, {'resources': instance_id,'region': region})
    return instance_id


# Hold EC2 instance attributes
class InstanceInfo(object):
    def __init__(self, inst_json):
        self.json = inst_json
        inst = inst_json
        self.id = inst["InstanceId"]
        self.pub_ip = inst.get("PublicIpAddress", "noIP")
        tags_array = inst.get("Tags", [])
        self.state = inst.get('State', {}).get('Name').lower()
        self.tags_dict = dict([(d["Key"],d["Value"]) for d in tags_array])


def IterInstances(instance_id=None):
    params = {'region': None}
    if instance_id:
        params["instance-id"] = instance_id
    j = AwsSystem("aws ec2 describe-instances", params)
    if not j:
        return # nothing to iterate
    reservations = j.get("Reservations", [])
    for reserve in reservations:
        instances = reserve.get("Instances", [])
        for inst in instances:
            info = InstanceInfo(inst)
            if info.state in ['terminated', 'stopped']:
                continue
            yield info

def FindInstance(instance_id):
    for info in IterInstances(instance_id):
        return info
    return None


def GetPublicIp(instance_id):
    if not instance_id:
        return None
    for info in IterInstances(instance_id=instance_id):
        return info.pub_ip
    return None


def ListInstances():
    for inst in IterInstances():
        print "instance {0}: ip: {1}  tags: {2} state: {3}".format(inst.id, inst.pub_ip, str(inst.tags_dict), inst.state)

def DescribeInstance(instance_id):
    cmd = "aws ec2 describe-instance-status"
    params = {'instance-id': instance_id, 'region': None}
    return AwsSystem(cmd, params)

def SSHInstance(instance_id, pem_ppk_file):
    if sys.platform == "win32":
        SSHInstanceWin32(instance_id, pem_ppk_file)
    else:
        SSHInstanceUnix(instance_id, pem_ppk_file)

def SSHInstanceWin32(instance_id, ppk_file):
    # 'C:\Program Files (x86)\PuTTY\putty.exe'
    # -ssh $HOST -l user -i private-key-file
    putty = os.getenv("PUTTYEXE", 'C:\Program Files (x86)\PuTTY\putty.exe')
    if not os.path.exists(putty):
        print "ERROR - could not find putty exe : %s" % putty
        print "Please define env variable PUTTYEXE"
        return
    instance = FindInstance(instance_id)
    if not instance:
        print "instance not found:", instance_id
        return
    ports = getDefault('ssh-tunnel-ports')
    lflag = ""
    if not ppk_file or not os.path.exists(ppk_file):
        print "ERROR - no pem file found : ", ppk_file
        exit(3)
    for port in ports:
        if isinstance(port, tuple):
            src_port, dst_port = port
        else:
            src_port = port
            dst_port = port
        lflag += " -L %d:%s:%d" % (src_port, instance.pub_ip, dst_port)
    System("\"%s\" -ssh -l %s -i %s %s %s"% (putty, getDefault('ec2-username'), ppk_file, instance.pub_ip, lflag))


# ssh to instance from laptop - this does NOT work on Windows
def SSHInstanceUnix(instance_id, pem_file):
    instance = FindInstance(instance_id)
    if not instance:
        print "instance not found:", instance_id
        return
    ports = getDefault('ssh-tunnel-ports')
    lflag = ""
    if not pem_file or not os.path.exists(pem_file):
        print "ERROR - no pem file found : ", pem_file
        exit(3)
    for port in ports:
        lflag += " -L %d:%s:%d" % (port, instance.pub_ip, port)
    System("ssh -i %s %s@%s %s"% (pem_file, getDefault('ec2-username'), instance.pub_ip, lflag))

# ssh to instance from laptop - this does NOT work on Windows
def SCPInstances(instance_id, pem_file, src, dst):
    instance = FindInstance(instance_id, pem_file)
    if not instance:
        print "instance not found:", instance_id
        return
    if not pem_file or not os.path.exists(pem_file):
        print "ERROR - no pem file found : ", pem_file
        exit(3)
    System("scp -i %s %s@%s:%s" % (pem_file,src, getDefault('ec2-username'), instance.pub_ip, dst))


## EMR

def FindEMRClusterByNameTag(cluster_name):
    """Find cluster id by the value of the Name tag"""
    cmd = 'aws emr list-clusters'
    j = AwsSystem(cmd, {'region': None}, True)
    if not j:
        return None
    for c in j.get("Clusters", [{}]):
        state = c.get("Status", {}).get("State")
        if state in ['TERMINATING', 'TERMINATED', 'TERMINATED_WITH_ERRORS']:
            continue
        id = c.get("Id")
        name = c.get("Name")
        if name == cluster_name:
            return id
    return None

def DescribeEMRCluster(cluster_id):
    if not cluster_id:
        return None
    return AwsSystem("aws emr describe-cluster", {'cluster-id' : cluster_id, 'region': None})


def FindEMRClusterMasterInstance(cluster_id):
    if not cluster_id:
        return None
    j = AwsSystem("aws emr describe-cluster", {'cluster-id' : cluster_id, 'region': None})
    igroups = j.get("Cluster",{}).get("InstanceGroups",[])
    master_instance_group = None
    for ig in igroups:
        if ig.get("Name", "").lower().startswith("master"):
            master_instance_group = ig.get("Id")
    if not master_instance_group:
        return None
    j = AwsSystem("aws emr list-instances", {'cluster-id' : cluster_id, 'region': None}, True)
    master_instance = None
    for i in j.get("Instances", []):
        if i.get("InstanceGroupId") == master_instance_group:
            master_instance = i.get("Ec2InstanceId")
            break
    return master_instance


