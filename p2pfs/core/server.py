import threading
import socket
from abc import abstractmethod
import json
import struct
import logging
import zstandard as zstd
logger = logging.getLogger(__name__)


class MessageServer:
    def __init__(self, host, port):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind((host, port))
        self._process_lock = threading.Lock()
        self._compressor = zstd.ZstdCompressor()
        self._decompressor = zstd.ZstdDecompressor()

    def start(self):
        # put server listening into a thread
        threading.Thread(target=self._listen).start()
        self._server_started()

    def _listen(self):
        self._sock.listen(5)
        logger.info('Start listening on {}'.format(self._sock.getsockname()))
        while True:
            client, address = self._sock.accept()
            logger.info('New connection from {}'.format(address))
            self._client_connected(client)
            threading.Thread(target=self._read_message, args=(client,)).start()

    @staticmethod
    def __recvall(sock, n):
        """helper function to recv n bytes or return None if EOF is hit"""
        data = b''
        while len(data) < n:
            packet = sock.recv(n - len(data))
            if not packet:
                raise EOFError('peer socket closed')
            data += packet
        return data

    def _connect(self, ip, port):
        logger.info('Connecting to {}'.format((ip, port)))
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect((ip, port))
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
                raw_msg_len = self.__recvall(client, 4)
                msglen = struct.unpack('>I', raw_msg_len)[0]
                raw_msg = self.__recvall(client, msglen)
                msg = json.loads(self._decompressor.decompress(raw_msg).decode('utf-8'))
                logger.debug('Message {} from {}'.format(self.__message_log(msg), client.getpeername()))
                # process the packets in order
                # TODO: remove this lock for better parallelism
                self._process_lock.acquire()
                self._process_message(client, msg)
                self._process_lock.release()
        except EOFError:
            logger.warning('{} closed unexpectedly'.format(client.getpeername()))
            self._client_closed(client)

    def _write_message(self, client, message):
        assert isinstance(client, socket.socket)
        logger.debug('Writing {} to {}'.format(self.__message_log(message), client.getpeername()))
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


