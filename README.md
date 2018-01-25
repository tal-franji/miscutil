# miscutil
Useful scripts

## dirsync
File sync server/client
  Used to allow editing files on your laptop in local repo and reflecting changes on cloud machine
 server (--destination) is an HTTP server listening and allowing upload of files.
 --destination should be run on a cloud machine AWS/GCE to which you SSH
 client (--source) runs on your laptop and checks for file modification. When a file is
 modified - it checks with the server if it is newer and if so - uploads it.

```
 You should SSH to the machine with port forwarding to allow client/server communications.
 USAGE:
 [laptop]$ ssh -i <KEY.pem> <USER>@<HOST>  -L 8000:<HOST>:8000
 ... connecting
 [cloud]$ wget https://raw.githubusercontent.com/tal-franji/miscutil/master/dirsync.py
 [cloud]$ python dirsync.py --destination
 ... on a different window on your laptop:
 [laptop]$ cd <MY_REPO_DIR>
 [laptop]$ wget https://raw.githubusercontent.com/tal-franji/miscutil/master/dirsync.py
 [laptop]$ python dirsync.py --source

```


## awsutil
Functions to wrap around AWS CLI and handle EC2 instances
For example:

```
import awsutil

awsutil.setDefault('region', 'eu-west-1')

def ListInstances():
    for inst in awsutil.IterInstances():
        print "instance {0}: ip: {1}  tags: {2} state: {3}".format(inst.id, inst.pub_ip, str(inst.tags_dict), inst.state)

ListInstances()

```

## classfind
Help find Java class in a list of jars or paths

    Usage classfind.py  [-s class-name-sub-string]  jar_or_dir_1 jar_or_dir_2...
    Examples:
        # prints this help:
        $ classfind.py --help
        # use -s . to find ALL class and print their names
        $ classfind.py -s . /usr/local/hadoop/share/hadoop/common/lib/
        # find conflicts between jars in two directories
        $ classfind.py /usr/local/hadoop/share/hadoop/common/lib/ /usr/local/hadoop/share/hadoop/yarn/lib/
        # find conflicts in hadoop class path using the shell special `command` notation
        $ classfind.py  `hadoop classpath`
        # find specific class - note - case insensitive
        $ classfind.py  -s s3file `hadoop classpath` $SPARK_HOME/lib

###Output

```
$ classfind.py /usr/local/hadoop/share/hadoop/common/lib/ /usr/local/hadoop/share/hadoop/yarn/lib/

duplicate: class org.apache.commons.beanutils.converters.ShortConverter appears in files:
	/usr/local/hadoop/share/hadoop/common/lib/commons-beanutils-1.7.0.jar
	/usr/local/hadoop/share/hadoop/common/lib/commons-beanutils-core-1.8.0.jar
CONFLICT: class org.apache.commons.beanutils.converters.ShortConverter different versions in path commons-beanutils-1.7.0.jar;commons-beanutils-core-1.8.0.jar
duplicate: class org.apache.commons.beanutils.locale.LocaleConvertUtils appears in files:
	/usr/local/hadoop/share/hadoop/common/lib/commons-beanutils-1.7.0.jar
	/usr/local/hadoop/share/hadoop/common/lib/commons-beanutils-core-1.8.0.jar
CONFLICT: class org.apache.commons.beanutils.locale.LocaleConvertUtils different versions in path commons-beanutils-1.7.0.jar;commons-beanutils-core-1.8.0.jar
duplicate: class org.apache.commons.beanutils.ConvertUtils appears in files:
	/usr/local/hadoop/share/hadoop/common/lib/commons-beanutils-1.7.0.jar
```

