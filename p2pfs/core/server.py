from p2pfs.core.utils import MessageServer
import socket


class CentralServer(MessageServer):
    def __init__(self, host, port):
        super().__init__(host, port)
        self._peers = {}

    def _client_connected(self, client):
        assert isinstance(client, socket.socket)

    def _process_message(self, client, message):
        assert isinstance(client, socket.socket)
        print(message)

    def _client_closed(self, client):
        pass
