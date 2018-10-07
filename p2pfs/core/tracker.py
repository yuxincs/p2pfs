from p2pfs.core.server import MessageServer, MessageType
import logging
import json
import asyncio
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

    async def _process_connection(self, reader, writer):
        assert isinstance(reader, asyncio.StreamReader) and isinstance(writer, asyncio.StreamWriter)
        self._peers[writer] = None
        while not reader.at_eof():
            message = await self._read_message(reader)
            message_type = MessageType(message['type'])
            if message_type == MessageType.REQUEST_REGISTER:
                # peer_address is a string, since JSON requires keys being strings
                self._peers[writer] = json.dumps(message['address'])
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
                        self._peers[writer]: list(range(0, message['chunknum']))
                    }
                    await self._write_message(writer, {
                        'type': MessageType.REPLY_PUBLISH,
                        'filename': message['filename'],
                        'result': True,
                        'message': 'Success'
                    })
                    logger.info('{} published file {} of {} chunks'
                                .format(self._peers[writer], message['filename'], message['chunknum']))
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
                # TODO: merge the chunknum with the list
                if peer_address in self._chunkinfo[message['filename']]:
                    self._chunkinfo[message['filename']][peer_address].append(message['chunknum'])
                else:
                    self._chunkinfo[message['filename']][peer_address] = [message['chunknum']]
            else:
                logger.error('Undefined message with {} type, full packet: {}'.format(message['type'], message))

        writer.close()
        del self._peers[writer]
        await writer.wait_closed()
