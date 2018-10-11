from abc import abstractmethod
import json
import struct
import logging
import asyncio
from enum import Enum, auto
import zstandard as zstd
logger = logging.getLogger(__name__)


class MessageType(Enum):
    REQUEST_REGISTER = auto()
    REQUEST_PUBLISH = auto()
    REQUEST_FILE_LIST = auto()
    REQUEST_FILE_LOCATION = auto()
    REQUEST_CHUNK_REGISTER = auto()
    REPLY_REGISTER = auto()
    REPLY_FILE_LIST = auto()
    REPLY_PUBLISH = auto()
    REPLY_FILE_LOCATION = auto()
    PEER_REQUEST_CHUNK = auto()
    PEER_REPLY_CHUNK = auto()
    PEER_PING_PONG = auto()


class MessageServer:
    """ Base class for async TCP server, provides useful _read_message and _write_message methods
    for transferring message-based packets.
    """
    _SOCKET_TIMEOUT = 5

    def __init__(self, ):
        # internal server states
        self._is_running = False
        self._server_address = None

        # manage the connections
        self._writers = set()
        self._server = None

        self._compressor = zstd.ZstdCompressor()
        self._decompressor = zstd.ZstdDecompressor()

    def is_running(self):
        return self._is_running

    async def start(self, local_address, loop=None):
        logger.info('Start listening on {}'.format(self._server_address))
        # start server
        self._server = await asyncio.start_server(self.__new_connection, *local_address, loop=loop)
        # update server address, only get the first 2 elements because under IPv6 the return value contains 4 elements
        # see https://docs.python.org/3.7/library/socket.html#socket-families
        self._server_address = self._server.sockets[0].getsockname()[:2]
        self._is_running = True
        return True

    async def stop(self):
        logger.warning('Shutting down {}'.format(self))
        self._is_running = False
        self._server.close()
        await self._server.wait_closed()
        for writer in set(self._writers):
            writer.close()
            await writer.wait_closed()

        if len(self._writers) != 0:
            logger.warning('Writers not fully cleared {}'.format(self._writers))

        self._writers = set()

    @staticmethod
    def _message_log(message):
        log_message = {key: message[key] for key in message if key != 'data'}
        log_message['type'] = MessageType(message['type']).name
        return log_message

    async def _read_message(self, reader):
        assert isinstance(reader, asyncio.StreamReader)
        # receive length header -> decompress (bytes) -> decode to str (str) -> json load (dict)
        raw_msg_len = await reader.readexactly(4)
        msglen = struct.unpack('>I', raw_msg_len)[0]
        raw_msg = await reader.readexactly(msglen)

        msg = json.loads(self._decompressor.decompress(raw_msg).decode('utf-8'))
        logger.debug('Message received {}'.format(self._message_log(msg)))
        return msg

    async def _write_message(self, writer, message):
        assert isinstance(writer, asyncio.StreamWriter)
        logger.debug('Writing {}'.format(self._message_log(message)))
        # use value of enum since Enum is not JSON serializable
        if isinstance(message['type'], MessageType):
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
        try:
            await self._process_connection(reader, writer)
        except ConnectionResetError:
            # the peer has disconnected, thus the writer will raise ConnectionResetError
            # which is fine
            pass
        finally:
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            self._writers.remove(writer)

    @abstractmethod
    async def _process_connection(self, reader, writer):
        raise NotImplementedError
