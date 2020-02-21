import asyncio


class StorageError(Exception):
    pass


class Storage:
    def __init__(self):
        self._data = {}
        # 'palm.cpu': [
        #     (1150864247, 0.5),
        #     (1150864248, 0.5)
        #   ],

    def get(self, key):
        try:
            data = "ok\n"

            keys = self._data.keys()
            lenght = len(keys)

            if lenght == 0:
                data = "ok\n\n"
                return data

            elif key == "*":
                # get *\n
                for k in keys:
                    params = self._data[k]
                    # ok\npalm.cpu
                    for param in params:
                        # palm.cpu 2.0 1150864248\n
                        data += k + " " + param[1] + " " + param[0] + '\n'

                return data + "\n"

            else:
                params = self._data[key]
                # ok\npalm.cpu
                for param in params:
                    # palm.cpu 2.0 1150864248\n
                    data += key + " " + param[1] + " " + param[0] + '\n'

                return data + "\n"

        except ...:
            return "error\nwrong command\n\n"

    def put(self, key, value, timestamp):
        try:
            keys = self._data.keys()

            if key not in keys:
                self._data[key] = set()

            self._data[key].add((timestamp, value,))

            return "ok\n\n"

        except ...:
            return "error\nwrong command\n\n"


class ValidatorError(Exception):
    pass


class Validator:
    def __init__(self) -> None:
        super().__init__()

    def encode_data(self, data):
        try:
            return data.encode()

        except ...:
            raise ValidatorError("Invalid data")

    def decode_data(self, data):
        try:
            decode_data = data.decode()[:-1].split(' ')

            if self.validate(decode_data) is True:

                return decode_data

            else:
                raise ValidatorError("Invalid data")

        except ...:
            raise ValidatorError("Invalid data")

    @staticmethod
    def validate(decode_data):
        try:
            length = len(decode_data)

            if "get" in decode_data:
                if length != 2:
                    return False

                elif length == 2:

                    if decode_data[1] == "\n":
                        return False

                    else:
                        return True
                else:
                    return False

            elif "put" in decode_data:

                if length != 4:
                    return False

                elif length == 4:
                    if float(decode_data[2]) and int(decode_data[3]) and decode_data[1] != "\n":
                        return True

                    else:
                        return False

                else:
                    return False

            else:
                return False

        except ValidatorError as err:
            return False


class Server(asyncio.Protocol):
    def __init__(self) -> None:
        self._storage = Storage()
        self._validator = Validator()
        super().__init__()

    def connection_made(self, transport):
        self.transport = transport

    def data_received(self, data):
        try:
            validate_data = self._validator.decode_data(data)
            msg = ""

            if validate_data[0] == "get":
                key = validate_data[1]
                msg = self._storage.get(key)

            elif validate_data[0] == "put":
                key, value, timestamp = validate_data[1:]
                msg = self._storage.put(key, value, timestamp)

            else:
                msg = "error\nwrong command\n\n"

            self.transport.write(self._validator.encode_data(msg))

        except Exception as err:
            self.transport.write(self._validator.encode_data("error\nwrong command\n\n"))


def run_server(host, port):
    loop = asyncio.get_event_loop()
    coro = loop.create_server(
        Server,
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



if __name__ == '__main__':
    run_server('127.0.0.1', 8182)