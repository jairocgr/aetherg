#!/usr/bin/env python3
import os.path
import random
import time
import atexit
import subprocess
import socket

from aether import AetherClient
from test_utils import SpawnThread, JsonArrayString
from test_utils import eprint

DEFAULT_MASTER_PORT = 27025  # Listen port for the master instance
MAX_CHECKS_ALLOWED = 64

SNAPSHOT = "local/tested_snapshot.snap"  # Path snapshot file

MAX_THREADS = 64  # Number of threads
KEYS_PER_THREAD = 128 * 2  # Number of keys worked on per thread
TIMEOUT_PER_THREAD = KEYS_PER_THREAD  # Max duration time per threads
RANDOM_STRING_FACTOR = 8  # Multiplier used to determine the test string size


def modify_server():
    writer = AetherClient("localhost", DEFAULT_MASTER_PORT, "WRITER")
    writer.connect()
    writer.test_command("SET 'EF9921FC2C487' '653861AA7B5B79ABA3A597BD32895'", ["+OK"])
    writer.test_command("SET '335DA' 'A8B399A4BD77FF3C'", ["+OK"])
    writer.test_command("RM 5F7FC", ["+OK"])
    writer.close()


def test_modified_server():
    print("Testing if snapshot was proper taken")
    tester = AetherClient("localhost", DEFAULT_MASTER_PORT, "TESTER-{}".format(DEFAULT_MASTER_PORT))
    tester.connect()
    tester.test_command("LIST", [
        "$41",
        JsonArrayString(["93759", "CFE46", "EF9921FC2C487", "335DA"]),
    ])
    tester.test_command("GET 93759", [
        "$29",
        "2683D7C52C1FAD6CB7F127F572754"
    ])
    tester.test_command("GET CFE46", [
        "$30",
        "CF232455424FCA3E59DA3EFB2192AE"
    ])
    tester.test_command("GET EF9921FC2C487", [
        "$29",
        "653861AA7B5B79ABA3A597BD32895"
    ])
    tester.test_command("GET 335DA", [
        "$16",
        "A8B399A4BD77FF3C"
    ])
    tester.close()


def terminate(proc: subprocess.Popen):
    print("Terminating {}".format(proc.pid))
    proc.terminate()


def spawn_master(port):
    proc = subprocess.Popen(["./aetherg", "-p", str(port), "-f", SNAPSHOT, "-l", "info"])
    atexit.register(terminate, proc)
    checks = 0
    while True:
        a_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        location = ("localhost", port)
        check = a_socket.connect_ex(location)
        if check == 0:
            return proc
        checks += 1
        if check > MAX_CHECKS_ALLOWED:
            raise Exception("Master server timeout")
        time.sleep(0.1)


def run_server():
    global master_proc
    print("Running servers for snapshot testing (server localhost:{})".format(DEFAULT_MASTER_PORT))
    print("Running ./aetherg process at port {}".format(DEFAULT_MASTER_PORT))
    master_proc = spawn_master(DEFAULT_MASTER_PORT)


def prepare_initial_snapshot():
    print("Writing initial {}".format(SNAPSHOT))
    snap = open(SNAPSHOT, 'w')
    items = [
        ("5F7FC", "5.4-qq6C8$t~PÇ_Z?9;l|", 21+1),  # Add 1 for the cedilla
        ("93759", "2683D7C52C1FAD6CB7F127F572754", 29),
        ("CFE46", "CF232455424FCA3E59DA3EFB2192AE", 30),
    ]
    snap.write("# snapshot testing\n")
    for item in items:
        key = item[0]
        val = item[1]
        size = item[2]
        data = "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n".format(len(key), key, size, val)
        snap.write(data)
    snap.close()


def test_init_snapshot():
    print("Testing if initial snapshot was proper loaded")
    tester = AetherClient("localhost", DEFAULT_MASTER_PORT, "TESTER-{}".format(DEFAULT_MASTER_PORT))
    tester.connect()
    tester.test_command("LIST", [
        "$25",
        JsonArrayString(["5F7FC", "93759", "CFE46"]),
    ])
    tester.test_command("GET 5F7FC", [
        "$22",
        "5.4-qq6C8$t~PÇ_Z?9;l|"
    ])
    tester.test_command("GET 93759", [
        "$29",
        "2683D7C52C1FAD6CB7F127F572754"
    ])
    tester.test_command("GET CFE46", [
        "$30",
        "CF232455424FCA3E59DA3EFB2192AE"
    ])
    tester.close()


def create_dir_to(filepath):
    abs = os.path.abspath(filepath)
    dir = os.path.dirname(abs)
    os.makedirs(dir, exist_ok=True)


def stop_server():
    global master_proc
    print("Stopping master server (pid {})".format(master_proc.pid))
    master_proc.terminate()


def main():
    create_dir_to(SNAPSHOT)
    prepare_initial_snapshot()
    run_server()
    test_init_snapshot()
    modify_server()
    stop_server()
    run_server()
    test_modified_server()
    print("Test is done ✓")


if __name__ == "__main__":
    main()
