"""
This tests both the server and the client, by uppercasing the content of
different files.
"""
import os
import tempfile
import threading
import time

from jqueue import quickstart, server

# These are the names of the jobs, as well as their content
IN_FILES = {
    'A': 'a lowercase test',
    'B': 'miXeD-caSe',
    'C': 'UPPER CASE'
}

OUT_FILES = {
    'A.result': 'A LOWERCASE TEST',
    'B.result': 'MIXED-CASE',
    'C.result': 'UPPER-CASE'
}

svr = server.Server()

def server_thread_runner():
    svr.run([fname.encode('ascii') for fname in IN_FILES])

def client_thread_runner():
    def handler(data):
        return data.upper()

    quickstart.process_jobs('localhost', handler, ttl=5)

with tempfile.TemporaryDirectory() as tmpdir:
    os.chdir(tmpdir)
    for fname in IN_FILES:
        with open(fname, 'w') as fstream:
            fstream.write(IN_FILES[fname])

    server_thread = threading.Thread(target=server_thread_runner, name='Server')
    client_threads = [
        threading.Thread(target=client_thread_runner, name='Client')
        for _ in range(2)]

    server_thread.start()
    for thread in client_threads:
        thread.start()

    server_thread.join()
    for thread in client_threads:
        thread.join()

    for fname in OUT_FILES:
        with open(fname) as fstream:
            content = fstream.read()
            print('[{}]'.format(fname), repr(content), '==', repr(OUT_FILES[fname]))

    input('Press Enter to continue')
