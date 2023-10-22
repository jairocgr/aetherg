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

DEFAULT_MASTER_PORT = 27015  # Listen port for the master instance
MAX_CHECKS_ALLOWED = 64

SNAPSHOT = "local/parallel.snap"  # Path snapshot file

MAX_THREADS = 64  # Number of threads
KEYS_PER_THREAD = 128 * 2  # Number of keys worked on per thread
TIMEOUT_PER_THREAD = KEYS_PER_THREAD  # Max duration time per threads
RANDOM_STRING_FACTOR = 8  # Multiplier used to determine the test string size


def gen_string(string_size):
    random_string = ''
    for _ in range(string_size):
        # Random int from ascii 65 to 90
        random_integer = random.randint(65, 90)
        random_string += (chr(random_integer))
    return str(random_string)


def read_and_write(spawn_thread):
    thread_no = spawn_thread.number

    print("Inside read_and_write ({})".format(spawn_thread.name))

    read_port = DEFAULT_MASTER_PORT + 1 if thread_no % 2 == 0 else DEFAULT_MASTER_PORT + 2
    reader = AetherClient("localhost", read_port, "READER-{}".format(thread_no))
    reader.verbose = False
    reader.connect()

    writer = AetherClient("localhost", DEFAULT_MASTER_PORT, "WRITER-{}".format(thread_no))
    writer.verbose = False
    writer.connect()

    print("Working with {} keys ({})".format(KEYS_PER_THREAD, spawn_thread.name))

    key_prefix = "key__{}".format(thread_no)

    for i in range(0, KEYS_PER_THREAD):
        key = key_prefix + "__" + str(i)
        string = gen_string(i * RANDOM_STRING_FACTOR)
        writer.test_command("SET " + key + "  '" + string + "'", ["+OK"])
        time.sleep(0.01)
        reader.test_command("GET " + key, [
            "$" + (str(len(string))),
            string
        ])

    for i in range(0, KEYS_PER_THREAD):
        key = key_prefix + "__" + str(i)
        writer.test_command("rm " + key, ["+OK"])
        time.sleep(0.01)
        reader.test_command("GET " + key, [
            "-ERR Key \"{}\" not found".format(key)
        ])

    writer.close()
    reader.close()

    print("read_and_write ended ({})".format(spawn_thread.name))


def spawn_pairs(npairs):
    global threads
    threads = []
    for thread_no in range(npairs):
        t = SpawnThread(thread_no, read_and_write)
        t.start()
        threads.append(t)


def wait4all():
    for i, thread in enumerate(threads):
        thread.join(TIMEOUT_PER_THREAD)


def print_results():
    failures = []
    for i, thread in enumerate(threads):
        if not thread.success:
            failures.append(thread)

    if len(failures) == 0:
        print("SUCCESS: All {} threads end successfully!".format(len(threads)))
    else:
        for i, thread in enumerate(failures):
            eprint("FAIL: {} fail".format(thread.name))
        failmsg = "{} threads of {} failed!".format(len(failures), len(threads))
        eprint("FAIL: {}".format(failmsg))
        exit(64)


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


def spawn_replica(master_port, offset):
    port = master_port + offset
    proc = subprocess.Popen(
        ["./aetherg", "-p", str(port), "-r", "-s", "localhost:{}".format(master_port), "-l", "info"])
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
            raise Exception("Spawn server {} timeout".format(port))
        time.sleep(0.1)


def run_servers():
    print("Running servers for parallel testing (server localhost:{})".format(DEFAULT_MASTER_PORT))
    print("Running ./aetherg process at port {}".format(DEFAULT_MASTER_PORT))
    spawn_master(DEFAULT_MASTER_PORT)
    spawn_replica(DEFAULT_MASTER_PORT, 1)
    spawn_replica(DEFAULT_MASTER_PORT, 2)


def prepare_initial_snapshot():
    print("Writing initial {}".format(SNAPSHOT))
    snap = open(SNAPSHOT, 'w')
    items = [
        ("5F7FC", "5.4-qq6C8$t~PÇ_Z?9;l|", 21+1),  # Add 1 for the cedilla
        ("93759", "2683D7C52C1FAD6CB7F127F572754", 29),
        ("CFE46", "CF232455424FCA3E59DA3EFB2192AE", 30),
    ]
    snap.write("# parallel testing initial snapshot\r\n")
    for item in items:
        key = item[0]
        val = item[1]
        size = item[2]
        data = "*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n".format(len(key), key, size, val)
        snap.write(data)
    snap.close()


def test_initial_snapshot(port):
    tester = AetherClient("localhost", port, "TESTER-{}".format(port))
    tester.verbose = False
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


def test_replication():
    print("Testing if initial snapshot was proper SYNCed")
    test_initial_snapshot(DEFAULT_MASTER_PORT)
    test_initial_snapshot(DEFAULT_MASTER_PORT+1)
    test_initial_snapshot(DEFAULT_MASTER_PORT+2)


def create_dir_to(filepath):
    abs = os.path.abspath(filepath)
    dir = os.path.dirname(abs)
    os.makedirs(dir, exist_ok=True)


def main():
    create_dir_to(SNAPSHOT)
    prepare_initial_snapshot()
    run_servers()
    test_replication()
    spawn_pairs(MAX_THREADS)
    wait4all()
    print_results()


if __name__ == "__main__":
    main()
