#!/usr/bin/python
# File sync server/client
#  Used to allow editing files on your laptop in local repo and reflecting changes on cloud machine
# server (--destination) is an HTTP server listening and allowing upload of files.
# --destination should be run on a cloud machine AWS/GCE to which you SSH
# client (--source) runs on your laptop and checks for file modification. When a file is
# modified - it checks with the server if it is newer and if so - uploads it.
#
# You should SSH to the machine with port forwarding to allow client/server communications.
# USAGE:
# [laptop]$ ssh -i <KEY.pem> <USER>@<HOST>  -L 8000:<HOST>:8000
# ... connecting
# [cloud]$ wget https://raw.githubusercontent.com/tal-franji/miscutil/master/dirsync.py
# [cloud]$ python dirsync.py --destination
# ... on a different window on your laptop:
# [laptop]$ cd <MY_REPO_DIR>
# [laptop]$ wget https://raw.githubusercontent.com/tal-franji/miscutil/master/dirsync.py
# [laptop]$ python dirsync.py --source

from __future__ import print_function
__author__ = "tal.franji@gmail.com"

import argparse
import json
import os
import re
import SimpleHTTPServer
import SocketServer
import sys
import time
import httplib, urllib
import urlparse

class FileSyncServer(SimpleHTTPServer.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        self.update_ts = 0
        self.root_dir = args[0]
        # Note SimpleHTTPServer.SimpleHTTPRequestHandler is an old-style class BAAA!
        SimpleHTTPServer.SimpleHTTPRequestHandler.__init__(self, *args[1:], **kwargs)
        # TODO(franji): Read timestamp from file

    def parse_params(self):
        url_parts = urlparse.urlparse(self.path)
        qs = url_parts[4]
        params0 = urlparse.parse_qs(qs)
        params = {}
        for k,v in params0.iteritems():
            params[k] = v[-1]
        return params

    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        params = self.parse_params()
        filename = params.get("file")
        fts = params.get("fts")
        if file and fts:
            return self.get_file_status(filename)
        self.wfile.write(json.dumps({"status": "error"}))

    def get_file_status(self, filename):
        full = os.path.join(self.root_dir, filename)
        if os.path.exists(full):
            mtime = os.path.getmtime(full)
        else:
            mtime = 0
        j = {"files": [{"file": filename, "fts": mtime}], "status": "ok"}
        self.wfile.write(json.dumps(j))

    def do_POST(self):
        try:
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            data = self.rfile.read()

            params0 = urlparse.parse_qs(data)
            params = {}
            for k,v in params0.iteritems():
                params[k] = v[-1]

            filename = params["file"]
            fts = params["fts"]
            if not filename:
                return
            content = params["content"]
            full = os.path.join(self.root_dir, filename)
            dir = os.path.split(full)[0]
            if dir and not os.path.isdir(dir):
                os.makedirs(dir)
            with open(full, "w+b") as f:
                f.write(content)
                print("UPDATED {} t={}".format(filename, fts))
        except:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"status": "error"}))

    def log_request(self, code='-', size='-'):
        pass # TODO(franji): add verbose mode?


def StartSyncServer(addr, port, root_dir):
    def ServerConstructorHelper(*args, **kwargs):
        return FileSyncServer(*([root_dir] + list(args)), **kwargs)

    Handler = ServerConstructorHelper
    httpd = SocketServer.TCPServer((addr, port), Handler)

    print("File Sync Server at port", port)
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()



def ClientRequestFileTime(addr, file):
    params = urllib.urlencode({"file": file, "fts": 0})
    headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
    conn = httplib.HTTPConnection(addr)
    conn.request("GET", "/?" + params, None, headers)
    r = conn.getresponse()
    return r.read()


def ClientUploadFile(addr, full, filename, mtime):
    content = None
    print("Uploading file: ", filename)
    try:
        with open(full, "rb") as f:
            content = f.read()
        if not content:
            print("ERROR reading file ", filename)
            return
        params = urllib.urlencode({"file": filename, "fts": mtime, "content": content})
        headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
        conn = httplib.HTTPConnection(addr)
        conn.request("POST", "/", params,  headers)
        r = conn.getresponse()
        print("Upload response: ", r.status, r.reason)
    except:
        print("ERROR uploading file", filename)


def relative_path(root_dir, dirpath, f):
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


def IterRelativePath(root_dir):
    # generate all files in the directories under root_dir
    # generate names relative to root_dir
    for dirpath, _, filenames in os.walk(root_dir):
        for f in filenames:
            filename = relative_path(root_dir, dirpath, f)
            yield filename


def iter_merge_infinite_loop(iter_builder1, iter_builder2):
    it1 = iter_builder1()
    it2 = iter_builder2()
    while True:
        try:
            x = it1.next()
            yield x
        except (StopIteration, RuntimeError):
            it1 = iter_builder1()
        try:
            x = it2.next()
            yield x
        except (StopIteration, RuntimeError):
            it2 = iter_builder2()


def StartSyncClient(port, root_dir, include_regex=[r".*\.(py|java|xml)$"], exclude_regex=None):
    pat_include = map(lambda r: re.compile(r), include_regex) if include_regex else None
    pat_exclude = map(lambda r: re.compile(r), exclude_regex) if exclude_regex else None
    def skip_file(filename):
        skip = True
        if pat_include:
            for r in pat_include:
                if r.match(filename):
                    skip = False
        if pat_exclude:
            for r in pat_exclude:
                if r.match(filename):
                    skip = True
        return skip

    addr = "localhost:%d" % port
    files_attr = {}

    def handle_file(filename):
        # return True, mtime if file needed upload
        # return False if not
        full = os.path.join(root_dir, filename)
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
        if client_first_look:
            js = ClientRequestFileTime(addr, filename)
            j = json.loads(js)
            fts = j.get("files",[{}])[0].get("fts",0)
            if fts >= mtime:
                # server already updated from previous run of client
                # no need to upload
                return False, mtime
        ClientUploadFile(addr, full, filename, mtime)
        return True, mtime

    log_count = 0
    speed = 1.0
    recently_changed = {}
    for filename in iter_merge_infinite_loop(lambda : IterRelativePath(root_dir),
                                             lambda: iter(recently_changed.viewkeys())):
        if skip_file(filename):
            continue
        time.sleep(0.1 * speed)
        speed = min(max(speed * 1.05, 0), 1.0)
        log_count += 1
        if log_count >= 50:
            print("Checking file ", filename)
            log_count = 0
        updated, mtime = handle_file(filename)
        if updated:
            recently_changed[filename] = mtime
            speed /= 2.0
        else:
            # check if need to remove from recently changed
            if filename in recently_changed and time.time() - mtime > 5 * 60:
                del recently_changed[filename]



def main():
    parser = argparse.ArgumentParser(description='File Sync server')
    parser.add_argument('--port', type=int,
                    help='Port server listens to', default=8000)
    parser.add_argument('--dir',
                    help='root directory from which to read (--source)/ write (--destination)', default=".")
    parser.add_argument('--source', action='store_true', help="Run this on the source of the files to sync")
    parser.add_argument('--destination', action='store_true', help="Run this on the destination machine")
    parser.add_argument('--rex_include',
                    help='regex of files to include in sync (can give several)',
                    default=[r".*\.(py|java|xml|scala)$"],
                    action='append')
    parser.add_argument('--rex_exclude',
                    help='regex of files to exclude from sync (can give several)',
                    default=[r"^\."],
                    action='append')
    args = parser.parse_args()
    if args.destination:
        StartSyncServer("", args.port, args.dir)
    elif args.source:
        StartSyncClient(args.port, args.dir, args.rex_include, args.rex_exclude)
    else:
        print("ERROR - must specify either --source or --destination")

    return 0


if __name__ == '__main__':
    sys.exit(main())