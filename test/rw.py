#!/usr/bin/env python3

import atexit
import socket
import subprocess
import time
import os

from aether import AetherClient
from test_utils import JsonArrayString

DEFAULT_MASTER_PORT = 27020  # Listen port for the master instance
MAX_CHECKS_ALLOWED = 64

SNAPSHOT = "local/rw.snap"  # Path snapshot file


def test():
    print("Running Reader & Writer testing (server localhost:{})".format(DEFAULT_MASTER_PORT))

    reader = AetherClient("localhost", DEFAULT_MASTER_PORT, "READER")
    reader.connect()

    writer = AetherClient("localhost", DEFAULT_MASTER_PORT, "WRITER")
    writer.connect()

    writer.test_command("PING", ["+PONG"])

    writer.test_command("SET short-lived  \"short lived value\"", ["+OK"])
    reader.test_command("GET 'short-lived'", [
        "$17",
        "short lived value"
    ])

    writer.test_command("RMALL", ["+OK"])  # Clean slate
    reader.test_command("LIST", [
        "$2",
        JsonArrayString([]),
    ])

    # Set 3 keys
    writer.test_command("SET key_0  value0", ["+OK"]),
    writer.test_command("SET key_1  value1", ["+OK"]),
    writer.test_command("SET key_2  '9ED323389Ç346'", ["+OK"]),

    # Check if the keys are being listed
    reader.test_command("LIST", [
        "$25",
        JsonArrayString(["key_0", "key_1", "key_2"])
    ])

    # Check if they are recorded correctly
    reader.test_command("GET key_1", [
        "$6",
        "value1"
    ])
    reader.test_command("GET key_0", [
        "$6",
        "value0"
    ])
    reader.test_command("GET key_2", [
        "$14",
        "9ED323389Ç346"
    ])

    # Remove two of those keys
    writer.test_command("RM key_0", ["+OK"])
    writer.test_command("RM key_1", ["+OK"])

    # Make sure they disappears
    reader.test_command("LIST", [
        "$9",
        JsonArrayString(["key_2"]),
    ])

    reader.test_command("GET key_0", [
        "-ERR Key \"{}\" not found".format("key_0")
    ])

    reader.close()
    writer.close()
    print("Test is done ✓")


def terminate(proc: subprocess.Popen):
    print("Terminating backgrounding process {}".format(proc.pid))
    proc.terminate()


def run_server():
    print("Running ./aetherg process at port {}".format(DEFAULT_MASTER_PORT))
    proc = subprocess.Popen(["./aetherg", "-p", str(DEFAULT_MASTER_PORT), "-f", SNAPSHOT, "-l", "INFO"])
    atexit.register(terminate, proc)
    checks = 0
    while True:
        a_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        location = ("localhost", DEFAULT_MASTER_PORT)
        check = a_socket.connect_ex(location)
        if check == 0:
            return
        checks += 1
        if check > MAX_CHECKS_ALLOWED:
            raise Exception("Test server timeout")
        time.sleep(0.1)


def create_dir_to(filepath):
    abs = os.path.abspath(filepath)
    dir = os.path.dirname(abs)
    os.makedirs(dir, exist_ok=True)


def main():
    create_dir_to(SNAPSHOT)
    run_server()
    test()


if __name__ == "__main__":
    main()
