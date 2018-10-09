import logging
import json
import asyncio
from p2pfs.core.server import MessageServer, MessageType
logger = logging.getLogger(__name__)


class Tracker(MessageServer):
    def __init__(self, host, port):
        super().__init__(host, port)
        # {writer -> address}
        self._peers = {}
        # {filename -> fileinfo(size, total_chunknum)}
        self._file_list = {}
        # {filename -> {(address) -> chunknum}}
        self._chunkinfo = {}

    def file_list(self):
        return self._file_list

    def chunkinfo(self):
        return self._chunkinfo

    def peers(self):
        return tuple(self._peers.values())

    async def _process_connection(self, reader, writer):
        assert isinstance(reader, asyncio.StreamReader) and isinstance(writer, asyncio.StreamWriter)
        logger.info('New connection from {}'.format(writer.get_extra_info('peername')))
        self._peers[writer] = None
        while not reader.at_eof():
            message = await self._read_message(reader)
            if message is None:
                break
            message_type = MessageType(message['type'])
            if message_type == MessageType.REQUEST_REGISTER:
                # peer_address is a string, since JSON requires keys being strings
                self._peers[writer] = json.dumps(message['address'])
                await self._write_message(writer, {
                    'type': MessageType.REPLY_REGISTER
                })
                logger.debug(self._peers.values())
            elif message_type == MessageType.REQUEST_PUBLISH:
                if message['filename'] in self._file_list:
                    await self._write_message(writer, {
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
                        self._peers[writer]: list(range(0, message['fileinfo']['total_chunknum']))
                    }
                    await self._write_message(writer, {
                        'type': MessageType.REPLY_PUBLISH,
                        'filename': message['filename'],
                        'result': True,
                        'message': 'Success'
                    })
                    logger.info('{} published file {} of {} chunks'
                                .format(self._peers[writer], message['filename'], message['fileinfo']['total_chunknum']))
            elif message_type == MessageType.REQUEST_FILE_LIST:
                await self._write_message(writer, {
                    'type': MessageType.REPLY_FILE_LIST,
                    'file_list': self._file_list
                })
            elif message_type == MessageType.REQUEST_FILE_LOCATION:
                await self._write_message(writer, {
                    'type': MessageType.REPLY_FILE_LOCATION,
                    'filename': message['filename'],
                    'fileinfo': self._file_list[message['filename']],
                    'chunkinfo': self._chunkinfo[message['filename']]
                })
            elif message_type == MessageType.REQUEST_CHUNK_REGISTER:
                peer_address = self._peers[writer]
                if peer_address in self._chunkinfo[message['filename']]:
                    if message['chunknum'] not in self._chunkinfo[message['filename']][peer_address]:
                        self._chunkinfo[message['filename']][peer_address].append(message['chunknum'])
                else:
                    self._chunkinfo[message['filename']][peer_address] = [message['chunknum']]
            else:
                logger.error('Undefined message: {}'.format(self._message_log(message)))

        peer_address = self._peers[writer]
        # iterate over chunkinfo and remove the chunks this peer has
        files_to_remove = []
        for filename, peer_possession_dict in self._chunkinfo.items():
            if peer_address in peer_possession_dict:
                del self._chunkinfo[filename][peer_address]
                if len(self._chunkinfo[filename]) == 0:
                    files_to_remove.append(filename)
        # remove file on tracker if no peers have that
        for key in files_to_remove:
            del self._chunkinfo[key]
            del self._file_list[key]

        del self._peers[writer]
