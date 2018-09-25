from p2pfs.core.utils import MessageServer
import socket
import logging
logger = logging.getLogger(__name__)


class PeerServer(MessageServer):
    def __init__(self, host, port, server, server_port):
        super().__init__(host, port)
        self._peers = {}
        try:
            self._server_sock = self._connect(server, server_port)
        except ConnectionRefusedError:
            logger.error('Server connection refused!')
            exit(1)

    def _client_connected(self, client):
        assert isinstance(client, socket.socket)
        self._peers[client.getpeername()] = client

    def _process_message(self, client, message):
        print(message)

    def _client_closed(self, client):
        assert isinstance(client, socket.socket)
