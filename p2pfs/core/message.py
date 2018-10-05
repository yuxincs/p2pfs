from enum import Enum, auto


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
