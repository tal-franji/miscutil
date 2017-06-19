#!/usr/bin/python
# SSH to EMR cluster and then allow another SSH to the Spark UI/ApplicationMaster


__author__ = 'tal.franji@gmail.com'

import json
import os
import re
import sys
import tempfile


try:
    import awsutil
except:
    print "ERROR - missing awsutil.py"
    print "Please download from: https://raw.githubusercontent.com/tal-franji/miscutil/master/awsutil.py"
    exit(3)

key_file = r""
if sys.platform == "win32":
    if not key_file.endswith(".ppk"):
        print "ERROR - update script key_file to point to PPK file"
        exit(3)
else:
    if not key_file.endswith(".pem"):
        print "ERROR - update script key_file to point to PEM file"
        exit(3)

awsutil.setDefault("region","us-east-1")
awsutil.setDefault('ssh-tunnel-ports', [9182, 8080, 8020, 8880, 8890, 9026, 8088, 18080, 8888, 16010, 20888])
awsutil.setDefault('ec2-username', 'hadoop')



def main(argv):
    if len(argv) < 2:
        print """USAGE: cuemr.py cmd  parameters
         comands:
                ssh ClueterName  -- ssh into the master
                findui pem_file  -- run on EMR master - ssh tunnel into the SparkUI machine
                """
        return 3
    cmd = argv[1]
    argv2 = None
    if len(argv) > 2:
        argv2 = argv[2]
    if cmd == "ssh": #------------
        if not argv2:
            print "ERROR - please give "
        cluster_id = awsutil.FindEMRClusterByNameTag(argv2)
        if not cluster_id:
            print "NO Cluster found"
            return 3
        master_addr = awsutil.FindEMRClusterMasterAddr(cluster_id)
        if not master_addr:
            print "ERROR -  could not find master ip"
            return
        awsutil.SSHAddr(master_addr, key_file)
    elif cmd == "findui":
        print "SSH to Spark UI machine - creating an SHH tunnel."
        if not argv2:
            print "ERROR - must supply a pem file for SSH"
            return 3
        awsutil.EmrSSHTunnelToSparkUI(argv2)
    else:
        print "BAD parameter"
        return 3
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))

