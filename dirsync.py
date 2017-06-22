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
    def __init__(self, root_dir, *args, **kwargs):
        self.update_ts = 0
        self.root_dir = root_dir
        # Note SimpleHTTPServer.SimpleHTTPRequestHandler is an old-style class BAAA!
        SimpleHTTPServer.SimpleHTTPRequestHandler.__init__(self,*args, **kwargs)
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
        file = params.get("file")
        fts = params.get("fts")
        if file and fts:
            return self.get_file_status(file, fts)
        self.wfile.write(json.dumps({"status": "error"}))

    def get_file_status(self, file, fts):
        filename = os.path.join(self.root_dir, file)
        if os.path.exists(filename):
            mtime = os.path.getmtime(filename)
        else:
            mtime = 0
        j = {"files": [{"file": file, "fts": mtime}], "status": "ok"}
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
            dir = os.path.split(filename)[0]
            if dir and not os.path.isdir(dir):
                os.makedirs(dir)
            with open(filename, "w+b") as f:
                f.write(content)
                print "UPDATED ", filename
        except:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"status": "error"}))

    def log_request(self, code='-', size='-'):
        pass # TODO(franji): add verbose mode?

def StartSyncServer(addr, port, root_dir):
    def ServerConstructorHelper(*args, **kwargs):
        return FileSyncServer(root_dir, *args, **kwargs)

    Handler = ServerConstructorHelper
    httpd = SocketServer.TCPServer((addr, port), Handler)

    print "File Sync Server at port", port
    try:
        httpd.serve_forever()
    except:
        httpd.server_close()



def ClientRequestFileTime(addr, file):
    params = urllib.urlencode({"file": file, "fts": 0})
    headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
    conn = httplib.HTTPConnection(addr)
    conn.request("GET", "/?" + params, None, headers)
    r = conn.getresponse()
    return r.read()


def ClientUploadFile(addr, filename, mtime):
    content = None
    print "Uploading file: ", filename
    try:
        with open(filename, "rb") as f:
            content = f.read()
        if not content:
            print "ERROR reading file ", filename
            return
        params = urllib.urlencode({"file": filename, "fts": mtime, "content": content})
        headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
        conn = httplib.HTTPConnection(addr)
        conn.request("POST", "/", params,  headers)
        r = conn.getresponse()
        print "Upload response: ", r.status, r.reason
    except:
        print "ERROR uploading file", filename


def relative_path(root_dir, dirpath, f):
    full = os.path.join(dirpath, f)
    if not root_dir:
        return full
    if not full.startswith(root_dir):
        print "ERROR - bad path for root", full
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


def StartSyncClient(port, root_dir, include_regex=[r".*\.(py|java|xml)$"], exclude_regex=None):
    pat_include = map(lambda r: re.compile(r), include_regex) if include_regex else None
    pat_exclude = map(lambda r: re.compile(r), exclude_regex) if include_regex else None
    addr = "localhost:%d" % port
    files_attr = {}
    log_count = 0
    while True:
        for filename in IterRelativePath(root_dir):
            skip_file = True
            if pat_include:
                for r in pat_include:
                    if re.match(r, filename):
                        skip_file = False
            if pat_exclude:
                for r in pat_exclude:
                    if not re.match(r, filename):
                        skip_file = True
            if skip_file:
                continue

            time.sleep(0.1)
            log_count += 1
            if log_count >= 50:
                print "Checking file ", filename
                log_count = 0

            mtime = os.path.getmtime(filename)
            client_first_look = False
            if filename in files_attr:
                last_mtime = files_attr[filename]["mtime"]
                if mtime <= last_mtime:
                    continue
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
                    continue
            ClientUploadFile(addr, filename, mtime)


def main():
    parser = argparse.ArgumentParser(description='File Sync server')
    parser.add_argument('--port', type=int,
                    help='Port server listens to', default=8000)
    parser.add_argument('--dir',
                    help='root directory from which to read (--source)/ write (--destination)', default=".")
    parser.add_argument('--source', action='store_true', help="Run this on the source of the files to sync")
    parser.add_argument('--destination', action='store_true', help="Run this on the destination machine")
    parser.add_argument('--rex_include',
                    help='regex of files to include in sync (can give several)', default=r".*\.(py|java|xml|scala)$",
                    action='append')
    parser.add_argument('--rex_exclude',
                    help='regex of files to exclude from sync (can give several)',
                    action='append')
    args = parser.parse_args()
    if args.destination:
        StartSyncServer("", args.port, args.dir)
    elif args.source:
        StartSyncClient(args.port, args.dir, args.rex_include, args.rex_exclude)
    else:
        print "ERROR - must specify either --source or --destination"

    return 0


if __name__ == '__main__':
    sys.exit(main())