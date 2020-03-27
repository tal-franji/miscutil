# Auto notebook (.ipynb) to python file convertor
# The following code, when run from a notebook - starts a process on the VM of the notebooks
# that copies all *.ipynb --> ./notepy/*.py
# The script does that automatically.
# USAGE
# 1. Run this paragraph
# 2. Create a new notebook - for example create test1.ipynb with print("Hellow world") in it
# 3. import notepy.test1
# You will see "Hello world" as a result of the import
# _APACHE_SPARK_ 
# If you are using Apache Spark:
#     you can add the module file to spark with the code:
#     sc.addPyFile(notepy.test1.__file__)
# _GOOGLE_COLAB_
# If you are using Google Colab
#     Script will try to mount your drive and as for login
#     Script assume notebooks are under "/content/drive/My Drive/Colab Notebooks"
#     It adds this to sys.path which allows same usage (as above):
#     import notepy.test1


import os
import re
import sys
import time


def paragraph_to_py_module(py_filename, spark_context=None, src_text=None, paragraph_index=-2):
    """Write the last evaluated notebook paragraph to a file and send to executors"""
    if src_text is None:
        src_text = In[paragraph_index]  # last paragraph==-2
    with open(py_filename, "w+t") as out_py:
        out_py.write(src_text)
    if spark_context:
        spark_context.addPyFile(py_filename)


def relative_path(root_dir, dirpath, f):
    """get the relative part of a file name """
    full = os.path.join(dirpath, f)
    if not root_dir:
        return full
    if not full.startswith(root_dir):
        print("ERROR - bad path for root", full)
        return None
    full = full[len(root_dir):]
    if full.startswith("/"):
        return full[1:]
    return full


def is_ipython():
    """check if running inside a notebook"""
    return 'get_ipython' in globals()


def ipython_kind():
    if not is_ipython():
        return None
    if "google.colab._shell.Shell" in str(get_ipython()):
        return "google_colab"
    if "spark" in globals():
        if "dbutils" in globals():
            return "spark_databricks"
        return "spark"
    return "unknown_ipython"


def google_colab_login_if_needed():
    gcolab_root = '/content/drive'
    if os.path.isdir(gcolab_root):
        return
    print("Need AUTH - mounting G-drive to access notebooks to export .ipynb->.py")
    from google.colab import drive
    drive.mount('/content/drive')


def do_system(cmd):
    print("Executing: ", cmd)
    if is_ipython():
        get_ipython().system_raw(cmd)
    else:
        os.system(cmd)
    
    
def iter_relative_path_recursive(root_dir):
    # generate all files in the directories under root_dir
    # generate names relative to root_dir
    for dirpath, _, filenames in os.walk(root_dir):
        for f in filenames:
            filename = relative_path(root_dir, dirpath, f)
            yield filename

            
def iter_relative_path(root_dir):
    for fname in os.listdir(root_dir):
        fullpath = os.path.join(root_dir, fname)
        if os.path.isdir(fullpath):
            continue
        yield fname

        
def iter_merge_infinite_loop(iter_builder1, iter_builder2):
    it1 = iter_builder1()
    it2 = iter_builder2()
    while True:
        try:
            x = it1.__next__()
            yield x
        except (StopIteration, RuntimeError):
            it1 = iter_builder1()
        try:
            x = it2.__next__()
            yield x
        except (StopIteration, RuntimeError):
            it2 = iter_builder2()

            
py_code_footer = """
__g_reload_module_called = False
__reload = None
def rerun_module(context=None):
    global __g_reload_module_called
    global __reload
    # context used for things like spark context
    if not __reload:
        try:
            __reload = reload  # v2.7
        except:
            pass
    if not __reload:
        try:
            import imp
            __reload = imp.reload  # v3.2
        except:
            pass
    if not __reload:
        try:
            import importlib
            __reload = importlib.reload  # v3.4+
        except:
            pass
    if not __g_reload_module_called:
        __g_reload_module_called = True
        return  # on the first import - code already executed.
    # reload this module
    import sys
    new_self = __reload(sys.modules[__name__])
    new_self.__g_reload_module_called = True    
"""


py_code_footer_spark = py_code_footer + """
    if context and hasattr(context, "sparkContext"):
        global spark
        global sc
        spark = context
        sc = context.sparkContext
        sc.addPyFile(__file__)
"""
            
def copy_note_to_py(note_full_path, note_name, dst_dir, kind):
    #
    dst_py = re.sub(r"(\.ipynb)?$", ".py", note_full_path) # file created by convert
    cmd = "jupyter nbconvert --to python {}".format(note_full_path)
    do_system(cmd)
    # now append extra code to py
    footer = py_code_footer
    if kind.startswith('spark'):
        footer = py_code_footer_spark
    with open(dst_py, "a+t") as pyfile:
        pyfile.write(footer)
    py_name = os.path.split(dst_py)[1]
    os.rename(dst_py, os.path.join(dst_dir, py_name))
    
 
def create_notepy_dir_if_needed(parent_dir="."):
    dst_dir = os.path.join(parent_dir, "notepy")
    if not os.path.isdir(dst_dir):
        os.mkdir(dst_dir)
        # create package init file
        package_init = os.path.join(dst_dir, "__init__.py")
        with open(package_init, "w+t") as pkg:
            pkg.write("# NotePY init\n")


    
def copy_local_notes_to_py(src_dir=".", kind="", dst_dir=None, exclude_notes=[]):
    if not dst_dir:
        dst_dir = os.path.join(src_dir, "notepy")
        if not os.path.isdir(dst_dir):
            os.mkdir(dst_dir)
        
    files_attr = {}

    def handle_file(filename):
        # return True, mtime if file needed upload
        # return False if not
        nonlocal files_attr
        now = int(time.time())
        full = os.path.join(src_dir, filename)
        if not os.path.exists(full):
            #file may have been deleted
            return False, now  # just ignore - not handling deletes
        mtime = os.path.getmtime(full)
        client_first_look = False
        if filename in files_attr:
            last_mtime = files_attr[filename]["mtime"]
            if mtime <= last_mtime:
                return False, mtime
        else:
            files_attr[filename] = {}
            client_first_look = True
        files_attr[filename]["mtime"] = mtime
        copy_note_to_py(full, filename, dst_dir, kind)
        return True, mtime        
        
    log_count = 0
    speed=1.0
    recently_changed = {}
    for filename in iter_merge_infinite_loop(lambda : iter_relative_path(src_dir),
                                             lambda: iter(recently_changed.keys())):
        if not filename.endswith(".ipynb"):
            continue
        if filename in exclude_notes:
            continue
        time.sleep(0.1 * speed)
        speed = min(max(speed * 1.05, 0), 1.0) # slow down
        log_count += 1
        if log_count >= 50:
            # dilute the log by X50 to preven too much output
            print("Checking file ", filename)
            log_count = 0
        updated, mtime = handle_file(filename)
        if updated:
            recently_changed[filename] = mtime
            # if updated - accelerate
            speed /= 2.0
        else:
            # check if need to remove from recently changed
            if filename in recently_changed and time.time() - mtime > 5 * 60:
                del recently_changed[filename]


def kill_prev_script(ps_pattern):
    pid = os.getpid()                                     
    do_system("ps ax | grep notesync.py | grep -v grep | awk '{print $1}'| grep -v %s | xargs kill " % pid)


def mount_cd_to_notebooks():
    cd_cmd = ""
    kind = ipython_kind()
    if kind == "google_colab":
        google_colab_login_if_needed()
        colab_root = "/content/drive/My Drive/Colab Notebooks"
        os.chdir(colab_root)
        if not colab_root in sys.path:
            # script will create "notepy" under colab_root
            #this allows importing from notepy.MYNOTEBOOK
            sys.path.append(colab_root)
    elif kind == "spark_databricks":
        # TODO(franji): find how to access the notebooks in databricks
        pass
    

def main():
    if sys.platform == "win32":
        raise NotImplemented("Windows is not supported at this stage")
    if is_ipython():
        save_dir = os.getcwd()
        mount_cd_to_notebooks()  # cd into notebook root dir
        # running in main notebook - save this paragraph to a py file
        create_notepy_dir_if_needed()
        paragraph_to_py_module("notepy/notesync.py", paragraph_index=-1)
        # now run myself on the notebook machine machine
        kind = ipython_kind() or "unknown"
        # Call myself (notesync.py) with 'kind' parameter.
        # TODO(franji): fix the case where 'spark' variable is not defined yet
        #    when this code is called (e.g. creating a session in the notebook code)
        do_system("python3 notepy/notesync.py %s &" % kind)
        os.chdir(save_dir)  # restore
        print("""
        Notebooks code is mirroored under notepy.*
        Now you can put function in separate notebook anduse python import
        #For example:
        _
        from notepy import my_util_notebook
        -
        #To reload/rerun module use:
        -
        my_util_notebook.rerun_module()
        _
        If you use Apache Spark notebook use my_util_notebook.rerun_module(spark)
        the file my_util_notebook.ipynb is exporterd to notepy/my_util_notebook.py
        automatically.
        """)
    else:
        # in main but not in notebook - in a script
        kill_prev_script("notesync.py")
        kind = ""
        if len(sys.argv) > 1:
            kind = sys.argv[1]
        copy_local_notes_to_py(kind=kind, exclude_notes=["notesync.ipynb"])


# Some util function unrelated to notebook->py mirroring
#def globals - a "macro" allowing putting all the imports in 
#  one function of another module/notebook
class DefGlobals(Exception):
    pass

def def_globals(f):
    """
    Decorator to allow putting global imports into a function.
    This allows putting common imports in on module/notebook and calling it from another.
    Fumnction must and by calling raise DefGlobals()
    >>> from notepy import notesync
    >>> @notesync.def_globals
    ... def import_all():
    ...     import sklearn
    ...     import numpy as np
    ...     import pandas as pd
    ...     raise notesync.DefGlobals()
    ...
    >>> import_all(globals())
    >>> pd
    <module 'pandas' from '/dist/lib/python3.7/site-packages/pandas/__init__.py'>

    """
    import sys
    def wrapped_f(global_ref):
        try:
            f()
            raise ValueError("ERROR def_globals: 'raise DefGlobals()' must be called!")
        except DefGlobals as dge:
            frame = sys.exc_info()[2]
            # goto inner function frame
            frame = frame.tb_next.tb_frame
            inner_locals = frame.f_locals
            # copy local variables (e.g. imported modules) 
            # into global space
            for k,v in inner_locals.items():
                global_ref[k] = v            
    return wrapped_f

if __name__ == "__main__":
    main()