from p2pfs.core.utils import MessageServer
import socket
import logging
logger = logging.getLogger(__name__)


class CentralServer(MessageServer):
    def __init__(self, host, port):
        super().__init__(host, port)
        self._peers = {}
        self._id = 0

    def _client_connected(self, client):
        assert isinstance(client, socket.socket)
        self._peers[client] = None
        logger.debug(self._peers.values())

    def _process_message(self, client, message):
        assert isinstance(client, socket.socket)
        print(message)

    def _client_closed(self, client):
        assert isinstance(client, socket.socket)
        del self._peers[client]
        logger.debug(self._peers.values())
