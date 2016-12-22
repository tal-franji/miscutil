#!/usr/bin/python

# find conflicts between jars/classes and allow finding class
import glob
import os
import re
import shlex
import subprocess
import sys




def OSys(cmd, verbose):
    if verbose:
        print 'Executing: ', cmd
    args = shlex.split(cmd)
    p = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=False)
    out, err = p.communicate()
    if err:
        if verbose:
            print "ERROR =============\n", err
        return None
    return out


def usage(message):
    print """%s
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



        You can also use `hadoop classpah` as parameter - it is split by ':'""" % message

# Accept paths arg with the following opeion examples:
# 1. arg="file1.jar"  - yield only this (if matching pat)
# 2. arg="path/to/dir" - yield all files in dir (matching pat if given)
# 3. arg="path/to/dir/*" -  a glob() pattern
# 4. arg=file1.jar:file2.jar:path1/dir/"  - a list of options 1-3 separated by :
def FileIter(arg, pattern):
    cpat = None
    paths = [arg]
    if ':' in arg:
        paths = arg.split(':')
    if pattern:
        cpat = re.compile(pattern, re.I)
        yield_pred = lambda file: cpat.search(file)
    else:
        yield_pred = lambda file: True

    for path in paths:
        if os.path.exists(path):
            if os.path.isdir(path):
                for file in os.listdir(path):
                    full = os.path.join(path, file)
                    if yield_pred(full):
                        yield full
            else:
                if yield_pred(path):
                    yield path
        else:
            # assume it is a glob
            for file in glob.glob(path):
                if yield_pred(file):
                    yield file


def IterClasses(jarfile):
    # using unzip and not jar since JDK is not installed
    text = OSys("unzip -l " + jarfile, False)
    if not text:
        return
    records = map(lambda s: re.split(r"\s+", s), text.splitlines())
    records = filter(lambda r: len(r) == 5 and r[4].find(".class") > 0, records)
    files = map(lambda r: r[4], records)
    for f in files:
        yield f


def main(argv):
    if len(argv) < 2 or re.match(r"-?-(h(elp)?|\?)",argv[1], re.I):
        usage("")
        return
    find_substring = None
    ai = 1
    if argv[1] == "-s":
        if len(argv) < 4:
            usage("missing sub string expression")
            return
        find_substring = argv[2].lower()
        ai = 3
    # class index - map between class name and list of files containing it
    class_index = {}
    for arg in argv[ai:]:
        for file in FileIter(arg, r"\.jar$"):
            for clz in IterClasses(file):
                class_name = clz.replace(".class", "")
                class_name = class_name.replace("/", ".")
                if find_substring and class_name.lower().find(find_substring) >= 0:
                    print class_name
                class_index.setdefault(class_name, []).append(file)
    # print conflicts:
    for name, files in class_index.iteritems():
        if find_substring:
            if name.lower().find(find_substring) >= 0:
                print "found: class {} appears in files:\n\t{}".format(name, "\n\t".join(files))
        else:
            # if not defined find string - print all conflicts
            if len(files) > 1:
                versions = set()
                for f in files:
                    nopath = os.path.basename(f)
                    versions.add(nopath)
                print "duplicate: class {} appears in files:\n\t{}".format(name, "\n\t".join(files))
                if len(versions) > 1:
                    print "CONFLICT: class {} different versions in path {}".format(name, ";".join(versions))



if __name__ == '__main__':
    sys.exit(main(sys.argv))
