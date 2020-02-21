import asyncio


class Storage:
    def __init__(self):
        self._data = {}

    def put(self, key, value, timestamp):
        if key not in self._data:
            self._data[key] = {}

        self._data[key][timestamp] = value



    def get(self, key):
        data = self._data

        if key != "*":
            data = {
                key: data.get(key, {})
            }

        result = {}
        for key, timestamp_data in data.items():
            result[key] = sorted(timestamp_data.items())

        return result


class ParseError(ValueError):
    pass


class Parser:
    def encode(self, responses):
        rows = []
        for response in responses:
            if not response:
                continue
            for key, values in response.items():
                for timestamp, value in values:
                    rows.append(f"{key} {value} {timestamp}")

        result = "ok\n"

        if rows:
            result += "\n".join(rows) + "\n"

        return result + "\n"

    def decode(self, data):
        parts = data.split("\n")
        commands = []
        for part in parts:
            if not part:
                continue

            try:
                method, params = part.strip().split(" ", 1)
                if method == "put":
                    key, value, timestamp = params.split()
                    commands.append(
                        (method, key, float(value), int(timestamp))
                    )
                elif method == "get":
                    key = params
                    commands.append(
                        (method, key)
                    )
                else:
                    raise ValueError("unknown method")
            except ValueError:
                raise ParseError("wrong command")

        return commands


class ExecutorError(Exception):
    pass


class Executor:
    def __init__(self, storage):
        self.storage = storage

    def run(self, method, *params):
        if method == "put":
            return self.storage.put(*params)
        elif method == "get":
            return self.storage.get(*params)
        else:
            raise ExecutorError("Unsupported method")


class ServerError(Exception):
    pass


class EchoServerClientProtocol(asyncio.Protocol):
    storage = Storage()

    def __init__(self):
        super().__init__()

        self.parser = Parser()
        self.executor = Executor(self.storage)
        self._buffer = b''

    def process_data(self, data):
        commands = self.parser.decode(data)

        responses = []
        for command in commands:
            resp = self.executor.run(*command)
            responses.append(resp)

        return self.parser.encode(responses)

    @staticmethod
    def validate(data):
        decode_data = data[:-1].split(' ')
        try:
            length = len(decode_data)

            if "get" in decode_data:
                if length != 2:
                    return False

                elif length == 2:

                    if decode_data[1] in ["\n",'',"",'\\n\r']:
                        return False

                    else:
                        return True
                else:
                    return False

            elif "put" in decode_data:

                if length != 4:
                    return False

                elif length == 4:
                    try:
                        if decode_data[1] != "\n":
                            float(decode_data[2])
                            int(decode_data[3])
                            return True
                        else:
                            return False
                    except ...:
                        return False

                else:
                    return False

            else:
                return False

        except ...:
            return False

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        resp = ""
        decoded_data = ""

        self._buffer += data

        try:
            decoded_data = self._buffer.decode()

        except UnicodeDecodeError:
            raise ServerError("UnicodeDecodeError")

        if not decoded_data.endswith('\n'):
            raise ServerError("decoded_data without \\n")

        self._buffer = b''

        try:
            if self.validate(data=decoded_data) is True:
                resp = self.process_data(decoded_data)

            else:
                raise ServerError("data is not valid")

        except (ParseError, ExecutorError, ValueError, ServerError, Exception) as err:
            self.transport.write('error\nwrong command\n\n'.encode())
            return

        self.transport.write(resp.encode())


def run_server(host, port):
    loop = asyncio.get_event_loop()
    coro = loop.create_server(
        EchoServerClientProtocol,
        host, port
    )
    server = loop.run_until_complete(coro)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


if __name__ == "__main__":
    run_server('127.0.0.1', 8888)
