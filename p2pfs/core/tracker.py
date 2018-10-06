from p2pfs.core.server import MessageServer, MessageType
import socket
import logging
import json
logger = logging.getLogger(__name__)


class Tracker(MessageServer):
    def __init__(self, host, port):
        super().__init__(host, port)
        self._peers = {}
        # {filename -> fileinfo}
        self._file_list = {}
        # {filename -> {(address) -> chunknum}}
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
            # peer_address is a string, since JSON requires keys being strings
            self._peers[client] = json.dumps(message['address'])
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
                    self._peers[client]: list(range(0, message['chunknum']))
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
            peer_address = self._peers[client]
            # TODO: merge the chunknum with the list
            if peer_address in self._chunkinfo[message['filename']]:
                self._chunkinfo[message['filename']][peer_address].append(message['chunknum'])
            else:
                self._chunkinfo[message['filename']][peer_address] = [message['chunknum']]
        else:
            logger.error('Undefined message with {} type, full packet: {}'.format(message['type'], message))

    def _client_closed(self, client):
        # TODO: hanlde client closed unexpectedly
        logger.warning('{} closed'.format(client.getpeername()))
        assert isinstance(client, socket.socket)
        del self._peers[client]
        logger.debug(self._peers.values())
