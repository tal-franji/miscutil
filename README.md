# miscutil
Useful scripts

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

#Output

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



