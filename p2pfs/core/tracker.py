from p2pfs.core.utils import MessageServer
from p2pfs.core.message import MessageType
import socket
import logging
import uuid
logger = logging.getLogger(__name__)


class Tracker(MessageServer):
    def __init__(self, host, port):
        super().__init__(host, port)
        self._peers = {}

        self._file_list = {}

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
        elif message['type'] == MessageType.REQUEST_PUBLISH:
            if message['filename'] in self._file_list:
                self._write_message(client, {
                    'type': MessageType.REPLY_PUBLISH,
                    'filename': message['filename'],
                    'result': False,
                    'message': 'Filename already existed on server!'
                })
            else:
                self._file_list[message['filename']] = {
                    'size': message['size'],
                    'chunkinfo': message['chunkinfo']
                }
                self._write_message(client, {
                    'type': MessageType.REPLY_PUBLISH,
                    'filename': message['filename'],
                    'result': True,
                    'message': 'Success'
                })
                logger.info('{} published file {}'.format(self._peers[client], message['filename']))
        elif message['type'] == MessageType.REQUEST_FILE_LIST:
            self._write_message(client, {
                'type': MessageType.REPLY_FILE_LIST,
                'file_list': self._file_list
            })

    def _client_closed(self, client):
        assert isinstance(client, socket.socket)
        del self._peers[client]
        logger.debug(self._peers.values())
