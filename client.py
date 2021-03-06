import bisect
import socket
import time


class ClientError(Exception):
    """класс исключений клиента"""
    pass


class Client:
    def __init__(self, host, port, timeout=None):
        self.host = host
        self.port = port
        self.timeout = timeout

        try:
            self.connection = socket.create_connection((host, port), timeout)
        except socket.error as err:
            raise ClientError("Cannot create connection", err)

    def _read(self):

        data = b""

        while not data.endswith(b"\n\n"):
            try:
                data += self.connection.recv(1024)
            except socket.error as err:
                raise ClientError("Error reading data from socket", err)

        return data.decode('utf-8')

    def _send(self, data):

        try:
            self.connection.sendall(data)
        except socket.error as err:
            raise ClientError("Error sending data to server", err)

    def put(self, key, value, timestamp=None):

        timestamp = timestamp or int(time.time())
        self._send(f"put {key} {value} {timestamp}\n".encode())
        raw_data = self._read()

        if raw_data == 'ok\n\n':
            return
        raise ClientError('Server returns an error')

    def get(self, key):

        self._send(f"get {key}\n".encode())
        raw_data = self._read()
        data = {}
        status, payload = raw_data.split("\n", 1)
        payload = payload.strip()

        if status != 'ok':
            raise ClientError('Server returns an error')

        if payload == '':
            return data

        try:

            for row in payload.splitlines():
                key, value, timestamp = row.split()
                if key not in data:
                    data[key] = []
                bisect.insort(data[key], ((int(timestamp), float(value))))

        except Exception as err:
            raise ClientError('Server returns invalid data', err)

        return data

    def close(self):

        try:
            self.connection.close()
        except socket.error as err:
            raise ClientError("Error. Do not close the connection", err)


if __name__ == '__main__':
    # '''
    # >>> client = Client("127.0.0.1", 8888, timeout=15)
    #
    # >>> client.put("palm.cpu", 0.5, timestamp=1150864247)
    #
    # >>> client.put("palm.cpu", 2.0, timestamp=1150864248)
    #
    # >>> client.put("palm.cpu", 0.5, timestamp=1150864248)
    #
    # >>> client.put("eardrum.cpu", 3, timestamp=1150864250)
    #
    # >>> client.put("eardrum.cpu", 4, timestamp=1150864251)
    #
    # >>> client.put("eardrum.memory", 4200000)
    #
    # >>> print(client.get("*"))
    # '''
    client = Client("127.0.0.1", 8182, timeout=150)

    # print(client.get("*"))
    # print(client.get("palm.cpu"))
    # print(client.put("palm.cpu", "aaa", timestamp=1150864247))
    # print(client.put("palm.cpu", 2.0, timestamp=1150864248))
    # print(client.put("palm.cpu", 0.5, timestamp=1150864248))
    # put test_same_timestamp 0.0 1503319740\n
    print(client.put("test_same_timestamp", 0.0, timestamp=1503319740))
    print(client.put("palm.cpu", 0.3, timestamp=1150864248))
    print(client.put("test_key", 12.0, timestamp=1503319740))
    # print(client.put("eardrum.cpu", 3, timestamp=1150864250))
    # print(client.put("eardrum.cpu", 4, timestamp=1150864251))
    print(client.get("test_key"))
    print(client.get("*"))

    # print(client.put("eardrum.memory", 4200000))
    # print(client.get("\n"))