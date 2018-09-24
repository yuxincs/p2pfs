import threading
import socket
from abc import abstractmethod
import json
import struct


class MessageServer:
    def __init__(self, host, port):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind((host, port))

    def listen(self):
        self._sock.listen(5)
        while True:
            client, address = self._sock.accept()
            self._client_connected(client, address)
            threading.Thread(target=self._read_message, args=(client,)).start()

    @staticmethod
    def _recvall(sock, n):
        """helper function to recv n bytes or return None if EOF is hit"""
        data = b''
        while len(data) < n:
            packet = sock.recv(n - len(data))
            if not packet:
                raise EOFError('peer socket closed')
            data += packet
        return data

    def _read_message(self, client):
        assert isinstance(client, socket.socket)
        try:
            while True:
                raw_msg_len = self._recvall(client, 4)
                msglen = struct.unpack('>I', raw_msg_len)[0]
                raw_msg = self._recvall(client, msglen)
                self._process_message(json.loads(raw_msg.decode('utf-8')))
        except EOFError:
            self._client_closed(client)

    def _write_message(self, client, message):
        assert isinstance(client, socket.socket)
        raw_msg = json.dumps(message).encode('utf-8')
        raw_msg = struct.pack('>I', len(raw_msg)) + raw_msg
        client.sendall(raw_msg)

    @abstractmethod
    def _client_connected(self, client, address):
        pass

    @abstractmethod
    def _process_message(self, client, message):
        pass

    @abstractmethod
    def _client_closed(self, client):
        pass


