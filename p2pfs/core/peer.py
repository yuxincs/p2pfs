from p2pfs.core.utils import MessageServer
import socket
import logging
logger = logging.getLogger(__name__)


class PeerServer(MessageServer):
    def __init__(self, host, port, server, server_port):
        super().__init__(host, port)
        self._peers = {}
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.connect((server, server_port))

    def _client_connected(self, client):
        assert isinstance(client, socket.socket)
        logger.info('{} connected!'.format(client.getpeername()))
        self._peers[client.getpeername()] = client

    def _process_message(self, client, message):
        print(message)

    def _client_closed(self, client):
        logger.info('{} closed!'.format())
