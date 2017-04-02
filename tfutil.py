import os
import re
import signal
import tensorflow as tf

"""TensorFlow utils
Utils to work with TensorFlow inside Jupyter
Plus some convenient wrappers for session to start TensorBoard, summary writers etc.
"""

class TensorBoardRunner(object):
    def __init__(self):
        self.process = None
        self.log_dir = "/tmp/tf/train/"

    def Clean(self):
        print "Cleaning log directory...", self.log_dir
        # TODO(franji): do this clean via Python , not os.
        os.system("rm -f %s/*" % self.log_dir)

    def RunOnce(self):
        import shlex
        import subprocess
        import atexit
        if self.process:
            return
        pid_file = '/tmp/tensorboard.pid'
        if os.path.exists(pid_file):
            try:
                with open(pid_file) as pidf:
                    pids = pidf.read()
                    if re.match(r"\d+", pids):
                        old_pid = int(pids)
                        print "attempting killing old TensorBoard pid ", old_pid
                        os.kill(old_pid, signal.SIGTERM)
                        self.Clean()  # clean to prevent data messup between processes
            except:
                pass  # ignore errors in killing old process
        print "Running TensorBoard"
        # when running inside python virtual environment -
        # starting a new process to run tensorboard may
        # result in a process running in a different virtual-environment
        # so we allow configuring the tensorboard command
        # via an environment variable
        # for example - if you have a virtualenv 'tensorflow' installed - create tbenv.sh:
        #  |#!/bin/sh
        #  |source ~/tensorflow/bin/activate
        #  |exec tensorboard  "$@"
        #  |
        # $ export RUN_TENSORBOARD=/path/to/tbenv.sh
        tbbin = os.getenv("RUN_TENSORBOARD", "tensorboard")
        cmd = "%s --logdir %s" % (tbbin, self.log_dir)
        debug_tensorboard = False
        if debug_tensorboard:
            # run TensorBord redirected to log file to be able to find problems
            # when running inside Jupyter
            debug_out = open("/tmp/tf/tftbdebug.log", "w+t")
            self.process = subprocess.Popen(shlex.split(cmd),  shell=False, stdout=debug_out, stderr=debug_out)
        else:
            self.process = subprocess.Popen(shlex.split(cmd),  shell=False)
        print "TensorBoard pid ", self.process.pid
        with open(pid_file, "w+t") as pidf:
            pidf.write("%d" % self.process.pid)

        # TODO(franji): unregister old at exit for old TFSessionWithInit
        atexit.register(self.Kill)
        if not self.process:
            exit("ERROR - cannot start TensorBoard")

    def Kill(self):
        if self.process:
            self.process.kill()
            self.process = None

    def CleanRestart(self):
        self.Kill()
        self.Clean()
        self.RunOnce()



global_tensor_board_runner = None
# Global function to be used outside of a TFSessionWithInit
def TensorBoardRestart():
    global global_tensor_board_runner
    if not global_tensor_board_runner:
        global_tensor_board_runner = TensorBoardRunner()
    global_tensor_board_runner.Clean()


class TFSessionWithInit():
    def __init__(self, write_tensorboard=True, interactive=False):
        self.interactive = interactive
        self.tensor_board = TensorBoardRunner()
        self.tensor_board.Clean()
        self.sess = tf.Session()
        self.tf_init = None
        self.saver = None
        self.is_restored = False
        self.train_writer = None
        self.summary_merged = None
        self.write_tensorboard = write_tensorboard
        if write_tensorboard:
            self.train_writer = tf.summary.FileWriter(self.tensor_board.log_dir, self.sess.graph)
            self.summary_merged = tf.summary.merge_all()
        self.run_count = 0

    def _try_init_vars_once(self):
        if self.is_restored:
            return
        if self.tf_init is None:
            try:
                self.tf_init = tf.global_variables_initializer()
                self.saver = tf.train.Saver()
                self.sess.run(self.tf_init)
            except ValueError:
                print "WARNING (tfutil) - no variables to initialize"
        if self.write_tensorboard:
            self.summary_merged = tf.summary.merge_all()


    def __enter__(self):
        self.sess.__enter__()
        self.coord = tf.train.Coordinator()
        self.threads = tf.train.start_queue_runners(coord=self.coord, sess=self.sess)
        return self

    def do_exit(self):
        self.__exit__(None, None, None)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.coord.request_stop()
        self.coord.join(self.threads)
        self.sess.__exit__(exc_type, exc_val, exc_tb)

    def run_nosumm(self, *args, **kwargs):
        return self.sess.run(*args, **kwargs)

    def run(self, *args, **kwargs):
        self._try_init_vars_once()
        self.run_count += 1
        if self.train_writer is None or self.summary_merged is None:
            result = self.run_nosumm(*args, **kwargs)
            # Run TensorBoard after the first run - so files are found
            if self.train_writer is not None:
                self.tensor_board.RunOnce()
            return result
        node_list = args[0]
        node_list = [self.summary_merged] + node_list
        new_args = [node_list] + list(args)[1:]
        result = self.sess.run(*new_args, **kwargs)
        # result[0] is the summaries merged
        self.train_writer.add_summary(result[0], self.run_count)
        # Run TensorBoard after the first run - so files are found
        if self.train_writer is not None:
            self.tensor_board.RunOnce()
        return result[1:]  # skip summaries


    def saveModel(self, model_file_path):
        save_path = self.saver.save(self.sess, model_file_path)
        print "Model saved in file: %s" % save_path

    def restoreModel(self, model_file_path):
        if self.saver is None:
            self.saver = tf.train.Saver()
        self.is_restored = True
        self.saver.restore(self.sess, model_file_path)


global_interactive_session = None
# Global function to be used outside of a TFSessionWithInit
def SessionRestart():
    global global_interactive_session
    if global_interactive_session:
        global_interactive_session.do_exit()
        global_interactive_session = None
    if not global_interactive_session:
        global_interactive_session = TFSessionWithInit(write_tensorboard=True,interactive=True)
        global_interactive_session.__enter__()
    return global_interactive_session




# inspired by
# https://gist.github.com/kylemcdonald/2f1b9a255993bf9b2629
import PIL.Image
from cStringIO import StringIO
import IPython.display
import numpy as np
def ShowPNG(a, width, height, color_planes=1):
    a = np.uint8(a)
    mode = [None, "L", None, "RGB", "RGBA"][color_planes]
    if mode is None:
        raise ValueError("bad color_plane parameter - 1,3,4 allowed.")
    if color_planes == 1:
        a = a.reshape((width, height))
    else:
        a = a.reshape((width, height, color_planes))
    f = StringIO()
    PIL.Image.fromarray(a).save(f, 'png')
    IPython.display.display(IPython.display.Image(data=f.getvalue()))