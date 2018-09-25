import threading
import socket
from abc import abstractmethod
import json
import struct
import logging
logger = logging.getLogger(__name__)


class MessageServer:
    def __init__(self, host, port):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind((host, port))
        self._process_lock = threading.Lock()
        self._connection_lock = threading.Lock()

    def listen(self):
        self._sock.listen(5)
        logger.info('Start listening on {}'.format(self._sock.getsockname()))
        while True:
            client, address = self._sock.accept()
            self._connection_lock.acquire()
            self._client_connected(client)
            self._connection_lock.release()
            logger.info('New connection from {}'.format(address))
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

    def _read_message(self, client):
        assert isinstance(client, socket.socket)
        try:
            while True:
                raw_msg_len = self.__recvall(client, 4)
                msglen = struct.unpack('>I', raw_msg_len)[0]
                raw_msg = self.__recvall(client, msglen)
                msg = json.loads(raw_msg.decode('utf-8'))
                logger.debug('Message {} from {}'.format(msg, client.getpeername()))
                # process the packets in order
                self._process_lock.acquire()
                self._process_message(client, msg)
                self._process_lock.release()
        except EOFError:
            logger.warning('{} closed unexpectedly'.format(client.getpeername()))
            self._connection_lock.acquire()
            self._client_closed(client)
            self._connection_lock.release()

    def _write_message(self, client, message):
        assert isinstance(client, socket.socket)
        logging.info('Writing {} to {}'.format(message, client.getpeername()))
        raw_msg = json.dumps(message).encode('utf-8')
        raw_msg = struct.pack('>I', len(raw_msg)) + raw_msg
        client.sendall(raw_msg)

    @abstractmethod
    def _client_connected(self, client):
        pass

    @abstractmethod
    def _process_message(self, client, message):
        pass

    @abstractmethod
    def _client_closed(self, client):
        pass


