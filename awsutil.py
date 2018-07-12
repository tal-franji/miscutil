#!/usr/bin/python
# Simple wrapper around AWS CLI interface to create EC2 instances etc.
# NO LICENCE , NO LIABILITIES
# this is FREE and you are allowed to copy, change, slice  it - do whatever you want
# this was written to Amazon Linux/CentOS
# You can find the latest version at:
# https://github.com/tal-franji/miscutil/blob/master/awsutil.py
__author__ = "tal.franji@gmail.com"

import datetime
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
    "emr_master_instance": "m3.xlarge",
    "emr_core_instance": "m3.xlarge",
    "emr_n_core": "2",
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


def AwsSystemRaw(cmd_head, params, useDefaultForNone=True):
    # ignore params where value is None
    effective_params = dict()
    for k,v in params.iteritems():
        if v is None:
            if useDefaultForNone:
                v = getDefault(k)
        if v is not None:
            effective_params[k] = v

    cmd = cmd_head + " " + " ".join(["--%s %s" % (k, v) for k,v in  effective_params.iteritems()])
    return OSys(cmd)



# add --key=value to the command based on params dict
# return parsed json result
def AwsSystem(cmd_head, params, useDefaultForNone=True):
    json_str = AwsSystemRaw(cmd_head, params, useDefaultForNone)
    if not json_str:
        return None
    try:
        j = json.loads(json_str)
        return j
    except:
        print "ERROR - Bad json from cmd=", cmd_head
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

def jpath(obj, path, default_value = None):
    a = path.split(".")
    match_index = re.compile(r'([\w_\-\d]+)\[(\d+)\]')
    cur = obj
    if cur is None:
        return default_value
    for key in a:
        if cur is None:
            return default_value
        m = match_index.match(key)
        if m:
            name = m.group(1)
            idx = int(m.group(2))
            cur = cur.get(name)
            if not cur:
                return default_value
            if not hasattr(cur, "__getitem__"):
                return None
            if idx >= len(cur):
                return default_value
            cur = cur[idx]
            continue
        # else - not indexing
        if not isinstance(cur, dict):
            return default_value
        cur = cur.get(key)
    return cur


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
    instance_id = jpath(j, "Instances[0].InstanceId")
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
        self.state = jpath(inst, 'State.Name', "").lower()
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


def SSHAddr(pub_addr, pem_ppk_file):
    if sys.platform == "win32":
        SSHAddrWin32(pub_addr, pem_ppk_file)
    else:
        SSHAddrUnix(pub_addr, pem_ppk_file)


def SSHInstanceWin32(instance_id, ppk_file):
    instance = FindInstance(instance_id)
    if not instance:
        print "instance not found:", instance_id
        return
    SSHAddrWin32(instance.pub_ip, ppk_file)

def SSHAddrWin32(pub_addr, ppk_file):
    # 'C:\Program Files (x86)\PuTTY\putty.exe'
    # -ssh $HOST -l user -i private-key-file
    putty = os.getenv("PUTTYEXE", 'C:\Program Files\PuTTY\putty.exe')
    if not os.path.exists(putty):
        putty = 'C:\Program Files\PuTTY\putty.exe'
    if not os.path.exists(putty):
        putty = 'C:\Program Files (x86)\PuTTY\putty.exe'
    if not os.path.exists(putty):
        print "ERROR - could not find putty exe : %s" % putty
        print "Please define env variable PUTTYEXE"
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
        lflag += " -L %d:%s:%d" % (src_port, pub_addr, dst_port)
    System("\"%s\" -ssh -l %s -i %s %s %s"% (putty, getDefault('ec2-username'), ppk_file, pub_addr, lflag))


# ssh to instance from laptop - this does NOT work on Windows
def SSHInstanceUnix(instance_id, pem_file):
    instance = FindInstance(instance_id)
    if not instance:
        print "instance not found:", instance_id
        return
    SSHAddrUnix(instance.pub_ip, pem_file)


def SSHAddrUnix(pub_addr, pem_file):

    ports = getDefault('ssh-tunnel-ports')
    lflag = ""
    if not pem_file or not os.path.exists(pem_file):
        print "ERROR - no pem file found : ", pem_file
        exit(3)
    for port in ports:
        lflag += " -L %d:%s:%d" % (port, pub_addr, port)
    i_flag = ""
    if pem_file:
        # pem file may be None if running from a machine with IAM role
        i_flag = "-i %s" % pem_file
    System("ssh %s %s@%s %s"% (i_flag, getDefault('ec2-username'), pub_addr, lflag))


# ssh to instance from laptop - this does NOT work on Windows
def SCPInstances(instance_id, pem_file, src, dst):
    instance = FindInstance(instance_id, pem_file)
    if not instance:
        print "instance not found:", instance_id
        return
    if not pem_file or not os.path.exists(pem_file):
        print "ERROR - no pem file found : ", pem_file
        exit(3)
    i_flag = ""
    if pem_file:
        # pem file may be None if running from a machine with IAM role
        i_flag = "-i %s" % pem_file
    System("scp %s %s@%s:%s" % (i_flag, src, getDefault('ec2-username'), instance.pub_ip, dst))


## EMR

def FindEMRClusterByNameTag(cluster_name):
    """Find cluster id by the value of the Name tag"""
    cmd = 'aws emr list-clusters'
    j = AwsSystem(cmd, {'region': None}, True)
    if not j:
        return None
    for c in j.get("Clusters", [{}]):
        state = jpath(c, "Status.State")
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

def TerminateEMRCluster(cluster_id):
    if not cluster_id:
        return None
    return AwsSystem("aws emr terminate-clusters", {'cluster-ids' : cluster_id, 'region': None})


def FindEMRClusterMasterAddr(cluster_id):
    if not cluster_id:
        return None
    j = AwsSystem("aws emr describe-cluster", {'cluster-id' : cluster_id, 'region': None})
    pub_dns = jpath(j, "Cluster.MasterPublicDnsName")
    if pub_dns:
        return pub_dns
    igroups = jpath(j, "Cluster.InstanceGroups",[])
    if not igroups:
        # cluster may have been created using instance groups or instance fleets
        igroups = jpath(j, "Cluster.InstanceFleets",[])
    if not igroups:
        print "ERROR - cannot find Cluster.InstanceGroups/Cluster.InstanceFleets"
        return None
    master_instance_group = None
    for ig in igroups:
        if ig.get("Name", "").lower().startswith("master"):
            master_instance_group = ig.get("Id")
    if not master_instance_group:
        return None
    j = AwsSystem("aws emr list-instances", {'cluster-id' : cluster_id, 'region': None}, True)
    master_instance = None
    for i in j.get("Instances", []):
        if i.get("InstanceGroupId") == master_instance_group or i.get("InstanceFleetId"):
            master_instance = i.get("Ec2InstanceId")
            break
    # find master address
    instance = FindInstance(master_instance)
    if not instance:
        print "MASTER instance not found:", master_instance
        return None
    return instance.pub_ip


def FindEMRClusterMasterInstance_deprecated_(cluster_id):
    if not cluster_id:
        return None
    j = AwsSystem("aws emr describe-cluster", {'cluster-id' : cluster_id, 'region': None})
    pub_dns = jpath(j, "Cluster.MasterPublicDnsName")
    if pub_dns:
        return pub_dns

    igroups = jpath(j, "Cluster.InstanceGroups",[])
    if not igroups:
        # cluster may have been created using instance groups or instance fleets
        igroups = jpath(j, "Cluster.InstanceFleets",[])
    if not igroups:
        print "ERROR - cannot find Cluster.InstanceGroups/Cluster.InstanceFleets"
        return None
    master_instance_group = None
    for ig in igroups:
        if ig.get("Name", "").lower().startswith("master"):
            master_instance_group = ig.get("Id")
    if not master_instance_group:
        return None
    j = AwsSystem("aws emr list-instances", {'cluster-id' : cluster_id, 'region': None}, True)
    master_instance = None
    for i in j.get("Instances", []):
        if i.get("InstanceGroupId") == master_instance_group or i.get("InstanceFleetId"):
            master_instance = i.get("Ec2InstanceId")
            break
    return master_instance


def EC2GetSpotBidPrice(instance_type, product_descriptions="Linux/UNIX", history_days=2):
    # find the max of last 7 days for the region
    now= datetime.datetime.now()
    t1 = now.isoformat()
    t0 = (now - datetime.timedelta(days=history_days)).isoformat()
    params = {'region': None,
              'start-time': t0,
              'end-time': t1,
              'instance-types': instance_type,
              'product-descriptions': product_descriptions,

              }
    j = AwsSystem("aws ec2 describe-spot-price-history", params, True)
    if not j:
        return None  # Can happen on timeout etc.
    ph = j.get('SpotPriceHistory', [])
    num_price_samples = 0
    total_prices = 0.0
    for p in ph:
            if p.get('AvailabilityZone', '').startswith(getDefault('region')):
                try:
                    price = float(p.get('SpotPrice'))
                    num_price_samples += 1
                    total_prices += price
                except:
                    pass

    avg_price = total_prices / num_price_samples
    return round(avg_price * 1.1, 3)  # bid a little higher. Only 3 digis after the decimal points are allowed


def ShowEMRCluster(cluster_id):
    """print Cluster status - human readable for DescribeEMRCluster result"""
    j = DescribeEMRCluster(cluster_id)
    if not j:
        print "ERROR - Cluster not found"
        return
    state = jpath(j, 'Cluster.Status.State', "UNKNOWN")
    message = jpath(j, 'Cluster.Status.StateChangeReason.Message', "")
    name = jpath(j, 'Cluster.Name', "UNKNOWN")
    print "Showing cluster named ", name
    print "Cluster status: ", state
    print message
    return state



def YarnFindSparkUI(yarn_master_ip):
    """Given yarn_master ip - find out where the application master for the first
    active application runs"""
    import urllib2
    master = "http://%s:8088/" % yarn_master_ip
    # get configured values

    api = master + "ws/v1/"

    appsJson = json.loads(urllib2.urlopen(api + "cluster/apps").read())
    tracking_url = None
    if appsJson.get('apps'):  # checks if 'apps' exists and has elements
        apps = appsJson['apps']['app']
        for app in apps:
            if not app:
                continue
            print "\nAPP: ", app['name']
            app_state = app['state']
            print "State: %s (queue %s)" % (app_state, app['queue'])
            print "allocatedMB %d" % (app['allocatedMB'])
            if app_state != "FINISHED":
                url = app['trackingUrl']
                tracking_url = url
        print
    else:
        print "NO APPS running - first activate Zeppelin or spark-shell and give them some work."

    return tracking_url


def EmrSSHTunnelToSparkUI(pem_file):
    import urlparse
    global master_address
    global machine_ip
    global ssh_tunnel_ports
    print "Assuming I'm running on EMR master cluster machine"
    print "Proxy Spark UI - Connecting YARN to find ApplicationMaster"
    spark_ui = YarnFindSparkUI(SelfInstancePubIp())
    if not spark_ui:
        print "ERROR - could not find YARN Spark application"
        return 3
    url = urlparse.urlparse(spark_ui)
    netloc = url.netloc or ""
    ui_host, ui_port = netloc.split(':')
    machine_ip = ui_host
    local_ui_port = 20888
    ssh_tunnel_ports = [(local_ui_port, int(ui_port))]
    new_tarcking_url = url._replace(netloc="localhost:%d" % local_ui_port)
    print "Spark UI SSH: ", "#" * 50
    print new_tarcking_url.geturl()
    print "Spark UI SSH: ", "^" * 50
    SSHAddr(machine_ip, pem_file)


