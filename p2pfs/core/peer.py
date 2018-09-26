from p2pfs.core.utils import MessageServer
from p2pfs.core.message import MessageType
import socket
import logging
import os.path
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

    def publish(self, file):
        if not os.path.exists(file):
            return False

    def _server_started(self):
        logger.info('Requesting to register')
        self._write_message(self._server_sock, {
            'type': MessageType.REQUEST_REGISTER,
            'address': self._sock.getsockname()
        })

    def _client_connected(self, client):
        assert isinstance(client, socket.socket)
        self._peers[client] = None
        logger.debug(self._peers.values())

    def _process_message(self, client, message):
        if message['type'] == MessageType.REPLY_REGISTER:
            logger.info('Successfully registered with id {}'.format(message['id']))
            for (id, (ip, port)) in message['peer_list']:
                client = self._connect(ip, port)
                self._peers[client] = id
                self._write_message(client, {
                    'type': MessageType.PEER_GREET,
                    'id': message['id']
                })
        elif message['type'] == MessageType.PEER_GREET:
            logger.info('Greetings from peer with id {}'.format(message['id']))
            assert client in self._peers
            self._peers[client] = message['id']

        logger.debug(self._peers.values())

    def _client_closed(self, client):
        assert isinstance(client, socket.socket)
        if client is self._server_sock:
            logger.error('Server {} closed unexpectedly'.format(client.getpeername()))
            exit(1)
        else:
            del self._peers[client]
        logger.debug(self._peers.values())
