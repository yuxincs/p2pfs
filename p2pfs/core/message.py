from enum import Enum, auto
import logging
import asyncio
import struct
import msgpack

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


def _message_log(message):
    log_message = {key: message[key] for key in message if key != 'data'}
    log_message['type'] = MessageType(message['type']).name
    return log_message


async def read_message(reader):
    assert isinstance(reader, asyncio.StreamReader)
    # receive length header -> msgpack load (dict)
    raw_msg_len = await reader.readexactly(4)
    msglen = struct.unpack('>I', raw_msg_len)[0]
    raw_msg = await reader.readexactly(msglen)

    msg = msgpack.loads(raw_msg)
    logger.debug('Message received {}'.format(_message_log(msg)))
    return msg


async def write_message(writer, message):
    assert isinstance(writer, asyncio.StreamWriter)
    logger.debug('Writing {}'.format(_message_log(message)))
    # use value of enum since Enum is not JSON serializable
    if isinstance(message['type'], MessageType):
        message['type'] = message['type'].value
    # msgpack (bytes) -> add length header (bytes)
    raw_msg = msgpack.dumps(message)
    raw_msg = struct.pack('>I', len(raw_msg)) + raw_msg
    writer.write(raw_msg)
    await writer.drain()
