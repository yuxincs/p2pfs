from abc import abstractmethod
import json
import struct
import logging
import zstandard as zstd
import asyncio
from enum import Enum, auto
logger = logging.getLogger(__name__)


class MessageType(Enum):
    REQUEST_REGISTER = auto()
    REQUEST_PUBLISH = auto()
    REQUEST_FILE_LIST = auto()
    REQUEST_FILE_LOCATION = auto()
    REQUEST_CHUNK_REGISTER = auto()
    REQUEST_LEAVE = auto()
    REPLY_REGISTER = auto()
    REPLY_FILE_LIST = auto()
    REPLY_PUBLISH = auto()
    REPLY_FILE_LOCATION = auto()
    REPLY_LEAVE = auto()
    PEER_REQUEST_CHUNK = auto()
    PEER_REPLY_CHUNK = auto()


class MessageServer:
    """ Base class for async TCP server, provides useful _read_message and _write_message methods
    for transferring message-based packets.
    """
    _SOCKET_TIMEOUT = 5

    def __init__(self, host, port, loop=None):
        self._server_address = (host, port)
        self._loop = loop

        self._compressor = zstd.ZstdCompressor()
        self._decompressor = zstd.ZstdDecompressor()

        # manage the connections
        self._writers = set()
        self._server = None

    async def start(self):
        logger.info('Start listening on {}'.format(self._server_address))
        # start server
        self._server = await asyncio.start_server(self.__new_connection, *self._server_address, loop=self._loop)
        # update server address, only get the first 2 elements because under IPv6 the return value contains 4 elements
        # see https://docs.python.org/3.7/library/socket.html#socket-families
        self._server_address = self._server.sockets[0].getsockname()[:2]
        return True

    async def stop(self):
        self._server.close()
        await self._server.wait_closed()
        for writer in set(self._writers):
            writer.close()
            await writer.wait_closed()

    @staticmethod
    def _message_log(message):
        log_message = {key: message[key] for key in message if key != 'data'}
        log_message['type'] = MessageType(message['type']).name
        return log_message

    async def _read_message(self, reader):
        assert isinstance(reader, asyncio.StreamReader)
        # receive length header -> decompress (bytes) -> decode to str (str) -> json load (dict)
        try:
            raw_msg_len = await reader.readexactly(4)
            msglen = struct.unpack('>I', raw_msg_len)[0]
            raw_msg = await reader.readexactly(msglen)
        except asyncio.IncompleteReadError:
            return None

        msg = json.loads(self._decompressor.decompress(raw_msg).decode('utf-8'))
        logger.debug('Message received {}'.format(self._message_log(msg)))
        return msg

    async def _write_message(self, writer, message):
        assert isinstance(writer, asyncio.StreamWriter)
        logger.debug('Writing {}'.format(self._message_log(message)))
        # use value of enum since Enum is not JSON serializable
        message['type'] = message['type'].value
        # json string (str) -> encode to utf8 (bytes) -> compress (bytes) -> add length header (bytes)
        raw_msg = json.dumps(message).encode('utf-8')
        compressed = self._compressor.compress(raw_msg)
        logger.debug('Compressed rate: {}'.format(len(compressed) / len(raw_msg)))
        compressed = struct.pack('>I', len(compressed)) + compressed
        writer.write(compressed)
        await writer.drain()

    async def __new_connection(self, reader, writer):
        self._writers.add(writer)
        await self._process_connection(reader, writer)
        self._writers.remove(writer)

    @abstractmethod
    async def _process_connection(self, reader, writer):
        raise NotImplementedError
