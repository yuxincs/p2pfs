import socket

MessageType = {
    # peer -> server
    'REQUEST_REGISTER': 1,
    'REQUEST_FILE_LIST': 2,
    'REQUEST_FILE_LOCATION': 3,
    'REQUEST_CHUNK_REGISTER': 4,
    'REQUEST_LEAVE': 5,

    # server -> peer
    'REPLY_REGISTER': 6,
    'REPLY_FILE_LIST': 7,
    'REPLY_FILE_LOCATION': 8,
    'REPLY_CHUNK_REGISTER': 9,
    'REPLY_LEAVE': 10,

    # peer <-> peer
    'PEER_REQUEST_CHUNK': 11,
    'PEER_REPLY_CHUNK': 12
}


def write_message(s, message):
    assert isinstance(s, socket.socket)
    # TODO


def read_message(s):
    assert isinstance(s, socket.socket)
    # TODO
