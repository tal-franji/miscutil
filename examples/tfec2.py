# example of using awsutil:
# Launch an EC2 instance with TensorFlow installed
__author__ = 'tal.franji@gmail.com'

import os
import re
import sys
import awsutil

# YOUR AWS account parameters
key_pair = "YOUR_KEY_PAIR_NAME"
# if working from windows - set here the file name of the ppk file for the key_pair
# if working in Mac/Linux - put here the name of the pem file to use
pem_or_ppk_file = r"/path/to/dot/pem/or/dot/ppk/file.pem"
# To allow SSH - you must specify your default subnet-id
# and the security-group-id that allows port 22 to connect
# TODO(franji): auto discover the subnet+security-group
subnet_id = "subnet-XXXXXXXX"
security_group_ids = "sg-XXXXXXXX"
awsutil.setDefault("region","eu-west-1") # Choose region where the above AMI is available


# Using Amazon Deep Learnin AMI in marketplace:
# https://aws.amazon.com/marketplace/pp/B01M0AXXQB
deep_learning_ami = "ami-6e5d6808" # AMI depends on region - see link above
# port 6006 is TensorBoard
awsutil.setDefault('ssh-tunnel-ports', [8888, 8080, 6006])

instance_types = [ "m3.xlarge", "p2.xlarge","p2.8xlarge","p2.16xlarge" ]

def GetUsername():
    u = os.getenv("USERNAME")
    if not u:
        u = os.getenv("USER")
    if not u:
        print "ERROR - env var USERNAME not defined"
        exit(3)
    return u

def InstanceTagValue():
    return "tensorflow-"+ GetUsername()

def main(argv):
    if len(argv) < 2:
        print """USAGE tfec2.py cmd [parameters]
        commands:
            start instance-type --  one of {}
            ssh  -- ssh into the machine (with tunnel)
            kill
            """.format(",".join(instance_types))
        return 3
    cmd = argv[1]
    argv2 = None
    if len(argv) > 2:
        argv2 = argv[2]
    if cmd == "start": #-----------
        instance_type = argv2
        if not instance_type in instance_types:
            print "ERROR - instance type not allowed for TensorFlow AMI"
            return 3
        awsutil.CreateInstanceIfNotExist(
            singelton_tag_name="kind", singelton_tag_value=InstanceTagValue(),
            image_id=deep_learning_ami, instance_type=instance_type,
            key_name=key_pair, subnet_id=subnet_id, security_group_ids=security_group_ids)
    elif cmd == "ssh": #------------
        instance = awsutil.InstanceOfTagValue("kind", InstanceTagValue())
        if not instance:
            print "ERROR - no instance"
        else:
            awsutil.SSHInstance(instance.id, pem_or_ppk_file)
    elif cmd == "show": #-----------
        instance = awsutil.InstanceOfTagValue("kind", InstanceTagValue())
        if not instance:
            print "ERROR - no instance"
        print instance.json
    elif cmd == "kill" or cmd == "stop": #-----------
        instance = awsutil.InstanceOfTagValue("kind", InstanceTagValue())
        if not instance:
            print "ERROR - no instance"
        else:
            params = {"instance-ids": instance.id,
                      "region": None}
            print "Kill ? Are you sure?"
            if not re.match(r"[Yy](es)?", raw_input("?>")):
                return 3
            print awsutil.AwsSystem("aws ec2 terminate-instances", params)
    else:
        print "BAD parameter"
        return 3
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))

