from enum import Enum, auto
import socket


class MessageType(Enum):
    # peer -> server
    REQUEST_REGISTER = auto()
    REQUEST_FILE_LIST = auto()
    REQUEST_FILE_LOCATION = auto()
    REQUEST_CHUNK_REGISTER = auto()
    REQUEST_LEAVE = auto()

    # server -> peer
    REPLY_REGISTER = auto()
    REPLY_FILE_LIST = auto()
    REPLY_FILE_LOCATION = auto()
    REPLY_CHUNK_REGISTER = auto()
    REPLY_LEAVE = auto()

    # peer <-> peer
    PEER_REQUEST_CHUNK = auto()
    PEER_REPLY_CHUNK = auto()


def write_message(s, message):
    assert isinstance(s, socket.socket)
    # TODO


def read_message(s):
    assert isinstance(s, socket.socket)
    # TODO
