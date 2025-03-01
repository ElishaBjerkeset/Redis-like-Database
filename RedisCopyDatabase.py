#Credit: Charles Leifer


from gevent import socket
from gevent.pool import Pool
from gevent.server import StreamServer

from collections import namedtuple
from io import BytesIO
from socket import error as socket_error
import logging


logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.DEBUG)


class CommandError(Exception): pass
class Disconnect(Exception): pass

Error = namedtuple('Error', ('message',))


class ProtocolHandler(object):
    def __init__(self):
        self.handlers = {
            b'+': self.handle_simple_string,
            b'-': self.handle_error,
            b':': self.handle_integer,
            b'$': self.handle_string,
            b'*': self.handle_array,
            b'%': self.handle_dict}

    def handle_request(self, socket_file):
        first_byte = socket_file.read(1)
        if not first_byte:
            raise Disconnect()
        
        logger.debug(f"Raw first byte received: {first_byte}")

        try:
            return self.handlers[first_byte](socket_file)
        except KeyError:
            logger.error(f"Unknown request type: {first_byte}")
            raise CommandError('bad request')

    def handle_simple_string(self, socket_file):
        return socket_file.readline().rstrip(b'\r\n').decode()

    def handle_error(self, socket_file):
        return Error(socket_file.readline().rstrip(b'\r\n').decode())

    def handle_integer(self, socket_file):
        return int(socket_file.readline().rstrip(b'\r\n'))

    def handle_string(self, socket_file):
        length = int(socket_file.readline().rstrip(b'\r\n'))
        if length == -1:
            return None
        length += 2
        return socket_file.read(length)[:-2].decode()

    def handle_array(self, socket_file):
        num_elements = int(socket_file.readline().rstrip(b'\r\n'))
        logger.debug(f"Array with {num_elements} elements")
        return [self.handle_request(socket_file) for _ in range(num_elements)]
    
    def handle_dict(self, socket_file):
        num_items = int(socket_file.readline().rstrip(b'\r\n'))
        elements = [self.handle_request(socket_file) for _ in range(num_items * 2)]
        return dict(zip(elements[::2], elements[1::2]))

    def write_response(self, socket_file, data):
        buf = BytesIO()
        self._write(buf, data)
        buf.seek(0)
        message = buf.getvalue()
        
        logger.debug(f"Sending response: {message}")

        socket_file.write(message)
        socket_file.flush()

    def _write(self, buf, data):
        if isinstance(data, str):
            data = data.encode('utf-8')

        if isinstance(data, bytes):
            buf.write(f"${len(data)}\r\n".encode())
            buf.write(data + b"\r\n")
        elif isinstance(data, int):
            buf.write(f":{data}\r\n".encode())
        elif isinstance(data, Error):
            buf.write(f"-{data.message}\r\n".encode())
        elif isinstance(data, (list, tuple)):
            buf.write(f"*{len(data)}\r\n".encode())
            for item in data:
                self._write(buf, item)
        elif isinstance(data, dict):
            buf.write(f"%{len(data)}\r\n".encode())
            for key in data:
                self._write(buf, key)
                self._write(buf, data[key])
        elif data is None:
            buf.write(b"$-1\r\n")
        else:
            raise CommandError(f"Unrecognized type: {type(data)}")


class Server(object):
    def __init__(self, host='127.0.0.1', port=31337, max_clients=64):
        self._pool = Pool(max_clients)
        self._server = StreamServer(
            (host, port),
            self.connection_handler,
            spawn=self._pool)

        self._protocol = ProtocolHandler()
        self._kv = {}

        self._commands = self.get_commands()

    def get_commands(self):
        return {
            'GET': self.get,
            'SET': self.set,
            'DELETE': self.delete,
            'FLUSH': self.flush,
            'MGET': self.mget,
            'MSET': self.mset}

    def connection_handler(self, conn, address):
        logger.info(f"Connection received: {address}")
        socket_file = conn.makefile('rwb')

        while True:
            try:
                raw_data = socket_file.peek().decode(errors='ignore')
                logger.debug(f"Raw request received: {raw_data}")

                data = self._protocol.handle_request(socket_file)
                logger.info(f"Parsed request: {data}")

            except Disconnect:
                logger.info(f"Client disconnected: {address}")
                break

            try:
                resp = self.get_response(data)
            except CommandError as exc:
                logger.exception("Command error")
                resp = Error(exc.args[0])

            self._protocol.write_response(socket_file, resp)

    def run(self):
        self._server.serve_forever()

    def get_response(self, data):
        if not isinstance(data, list):
            raise CommandError('Request must be list.')

        if not data:
            raise CommandError('Missing command')

        command = data[0].upper()
        if command not in self._commands:
            raise CommandError(f'Unrecognized command: {command}')
        else:
            logger.debug(f"Received command: {command}")

        return self._commands[command](*data[1:])

    def get(self, key):
        return self._kv.get(key)

    def set(self, key, value):
        self._kv[key] = value
        return 1

    def delete(self, key):
        return 1 if self._kv.pop(key, None) is not None else 0

    def flush(self):
        kvlen = len(self._kv)
        self._kv.clear()
        return kvlen

    def mget(self, *keys):
        return [self._kv.get(key) for key in keys]

    def mset(self, *items):
        for key, value in zip(items[::2], items[1::2]):
            self._kv[key] = value
        return len(items) // 2


class Client(object):
    def __init__(self, host='127.0.0.1', port=31337):
        self._protocol = ProtocolHandler()
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((host, port))
        self._fh = self._socket.makefile('rwb')

    def execute(self, *args):
        logger.debug(f"Client sending: {args}")
        self._protocol.write_response(self._fh, args)
        resp = self._protocol.handle_request(self._fh)
        if isinstance(resp, Error):
            raise CommandError(resp.message)
        return resp

    def get(self, key):
        return self.execute('GET', key)

    def set(self, key, value):
        return self.execute('SET', key, value)

    def delete(self, key):
        return self.execute('DELETE', key)

    def flush(self):
        return self.execute('FLUSH')

    def mget(self, *keys):
        return self.execute('MGET', *keys)

    def mset(self, *items):
        return self.execute('MSET', *items)


if __name__ == '__main__':
    from gevent import monkey; monkey.patch_all()
    Server().run()