# miscutil
Useful scripts

# classfind
Help find Java class in a list of jars or paths
Finds conflicts - same class in several different jars.
Example of usage:
$ classfind.py /usr/java/lib

For example on a Spark/Hadoop cluster this is useful for finding if and where a class is defined:
$ classfind.py -s s3file `hadoop class`