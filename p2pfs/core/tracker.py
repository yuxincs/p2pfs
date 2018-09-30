from p2pfs.core.server import MessageServer
from p2pfs.core.message import MessageType
import socket
import logging
import uuid
logger = logging.getLogger(__name__)


class Tracker(MessageServer):
    def __init__(self, host, port):
        super().__init__(host, port)
        self._peers = {}
        # {filename -> fileinfo}
        self._file_list = {}
        # {filename -> {id -> chunknum}}
        self._chunkinfo = {}

    def file_list(self):
        return self._file_list

    def chunkinfo(self):
        return self._chunkinfo

    def peers(self):
        return tuple(self._peers.values())

    def _client_connected(self, client):
        assert isinstance(client, socket.socket)
        self._peers[client] = None
        logger.debug(self._peers.values())

    def _process_message(self, client, message):
        assert isinstance(client, socket.socket)
        if message['type'] == MessageType.REQUEST_REGISTER:
            assert client in self._peers
            # assign an ID and reply with the peer list
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
                self._file_list[message['filename']] = message['fileinfo']
                # add to chunkinfo
                # TODO: optimize how the chunknums are stored
                self._chunkinfo[message['filename']] = {
                    self._peers[client][0]: list(range(0, message['chunknum']))
                }
                self._write_message(client, {
                    'type': MessageType.REPLY_PUBLISH,
                    'filename': message['filename'],
                    'result': True,
                    'message': 'Success'
                })
                logger.info('{} published file {} of {} chunks'
                            .format(self._peers[client], message['filename'], message['chunknum']))
        elif message['type'] == MessageType.REQUEST_FILE_LIST:
            self._write_message(client, {
                'type': MessageType.REPLY_FILE_LIST,
                'file_list': self._file_list
            })
        elif message['type'] == MessageType.REQUEST_FILE_LOCATION:
            self._write_message(client, {
                'type': MessageType.REPLY_FILE_LOCATION,
                'filename': message['filename'],
                'fileinfo': self._file_list[message['filename']],
                'chunkinfo': self._chunkinfo[message['filename']]
            })
        elif message['type'] == MessageType.REQUEST_CHUNK_REGISTER:
            peer_id, _ = self._peers[client]
            if peer_id in self._chunkinfo[message['filename']]:
                self._chunkinfo[message['filename']][peer_id].append(message['chunknum'])
            else:
                self._chunkinfo[message['filename']][peer_id] = [message['chunknum']]
        else:
            logger.error('Undefined message with {} type, full packet: {}'.format(message['type'], message))

    def _client_closed(self, client):
        # TODO: hanlde client closed unexpectedly
        assert isinstance(client, socket.socket)
        del self._peers[client]
        logger.debug(self._peers.values())
