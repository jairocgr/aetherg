import socket


class AetherClient:

    def __init__(self, host, port, name="default", verbose=True):
        self.host = host
        self.port = port
        self.name = name
        self.verbose = verbose

    def connect(self):
        self.sock = socket.socket(socket.AF_INET)
        self.sock.connect((self.host, self.port))

    def close(self):
        self.sock.close()

    def log(self, message):
        if self.verbose:
            print(message)

    def send(self, command):
        # Append command with the EOL pair
        cmd_line = command + "\r\n"
        self.log(">sending {}".format(cmd_line.encode()))
        self.sock.sendall(cmd_line.encode())

    def read_byte(self):
        bytes = self.sock.recv(1)
        return bytes[0]

    LINE_FEED = 10  # ASCII for line-feed (\n)

    def read_line(self):
        read_data = []
        while True:
            byte = self.read_byte()
            # self.log("<read byte {}".format(byte))
            read_data.append(byte)
            if byte == self.LINE_FEED:
                break
        self.log("<read bytes {}".format(read_data))
        str = bytes(read_data).decode("utf-8")
        self.log("<read string {}".format(str.encode()))
        return str

    def test_command(self, command, expected_lines):
        self.log(">testing command '{}' (client={})".format(command, self.name))
        self.send(command)
        for i, expected in enumerate(expected_lines):
            # expected = expected + "\r\n"
            recvd = self.read_line()
            recvd = recvd[:-2]
            self.log("<rcvd {}".format(recvd.encode()))
            self.log("<expc {}".format(expected.encode()))
            if recvd != expected:
                raise Exception("Expected {} received {}".format(expected.encode(), recvd.encode()))
