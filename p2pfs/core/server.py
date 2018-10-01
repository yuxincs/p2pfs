import threading
import socket
from abc import abstractmethod
import json
import struct
import logging
import zstandard as zstd
logger = logging.getLogger(__name__)


class MessageServer:
    _SOCKET_TIMEOUT = 5

    def __init__(self, host, port):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind((host, port))
        self._process_lock = threading.Lock()
        self._compressor = zstd.ZstdCompressor()
        self._decompressor = zstd.ZstdDecompressor()

        self._is_running = True

        # manage the connections
        self._connections_lock = threading.Lock()
        self._connections = set()

    def start(self):
        self._sock.listen(5)
        logger.info('Start listening on {}'.format(self._sock.getsockname()))
        # put server listening into a thread
        threading.Thread(target=self._listen).start()
        self._server_started()

    def stop(self):
        # shutdown the server
        self._is_running = False
        self._sock.close()
        # close all connections
        for client in self._connections:
            client.close()

    def _listen(self):
        try:
            while self._is_running:
                try:
                    client, address = self._sock.accept()
                    # add timeout to prevent waiting forever
                    client.settimeout(MessageServer._SOCKET_TIMEOUT)
                    logger.info('New connection from {}'.format(address))
                    with self._connections_lock:
                        self._client_connected(client)
                        self._connections.add(client)
                    threading.Thread(target=self._read_message, args=(client,)).start()
                except socket.timeout:
                    # ignore timeout exception
                    pass
        except (ConnectionAbortedError, OSError) as e:
            if self._is_running:
                # if exception occurred during normal execution
                logger.error(e)
            else:
                pass

    @staticmethod
    def __recvall(sock, n):
        """helper function to recv n bytes or raise exception if EOF is hit"""
        data = b''
        while len(data) < n:
            try:
                packet = sock.recv(n - len(data))
                if not packet:
                    raise EOFError('peer socket closed')
                data += packet
            except socket.timeout:
                pass
        return data

    def _connect(self, ip, port):
        logger.info('Connecting to {}'.format((ip, port)))
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((ip, port))
        client.settimeout(MessageServer._SOCKET_TIMEOUT)
        with self._connections_lock:
            self._connections.add(client)
        threading.Thread(target=self._read_message, args=(client,)).start()
        logger.info('Successfully connected to {} on {}'.format((ip, port), client.getsockname()))
        return client

    @staticmethod
    def __message_log(message):
        return {key: message[key] for key in message if key != 'data'} if 'data' in message else message

    def _read_message(self, client):
        assert isinstance(client, socket.socket)
        try:
            while True:
                # receive length header -> decompress (bytes) -> decode to str (str) -> json load (dict)
                raw_msg_len = self.__recvall(client, 4)
                msglen = struct.unpack('>I', raw_msg_len)[0]
                raw_msg = self.__recvall(client, msglen)
                msg = json.loads(self._decompressor.decompress(raw_msg).decode('utf-8'))
                logger.debug('Message {} from {}'.format(self.__message_log(msg), client.getpeername()))
                # process the packets in order
                # TODO: remove this lock for better parallelism
                with self._process_lock:
                    self._process_message(client, msg)
        except (EOFError, OSError):
            if self._is_running:
                logger.warning('{} closed'.format(client.getpeername()))
                with self._connections_lock:
                    assert client in self._connections
                    client.close()
                    self._connections.remove(client)
                    self._client_closed(client)

    def _write_message(self, client, message):
        assert isinstance(client, socket.socket)
        logger.debug('Writing {} to {}'.format(self.__message_log(message), client.getpeername()))
        # json string (str) -> encode to utf8 (bytes) -> compress (bytes) -> add length header (bytes)
        raw_msg = json.dumps(message).encode('utf-8')
        compressed = self._compressor.compress(raw_msg)
        logger.debug('Compressed rate: {}'.format(len(compressed) / len(raw_msg)))
        compressed = struct.pack('>I', len(compressed)) + compressed
        client.sendall(compressed)

    def _server_started(self):
        pass

    def _client_connected(self, client):
        pass

    @abstractmethod
    def _process_message(self, client, message):
        pass

    def _client_closed(self, client):
        pass


