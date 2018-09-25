from p2pfs.core.utils import MessageServer
from p2pfs.core.message import MessageType
import socket
import logging
import uuid
logger = logging.getLogger(__name__)


class CentralServer(MessageServer):
    def __init__(self, host, port):
        super().__init__(host, port)
        self._peers = {}

    def _client_connected(self, client):
        assert isinstance(client, socket.socket)
        self._peers[client] = None
        logger.debug(self._peers.values())

    def _process_message(self, client, message):
        assert isinstance(client, socket.socket)
        if message['type'] == MessageType.REQUEST_REGISTER:
            assert client in self._peers
            id = str(uuid.uuid4())
            self._write_message(client, {
                'type': MessageType.REPLY_REGISTER,
                'id': id,
                'peer_list': tuple(filter(lambda x: x is not None, self._peers.values()))
            })
            self._peers[client] = (id, message['address'])
            logger.debug(self._peers.values())

    def _client_closed(self, client):
        assert isinstance(client, socket.socket)
        del self._peers[client]
        logger.debug(self._peers.values())
