class MessageType:
    REQUEST_REGISTER, REQUEST_PUBLISH, REQUEST_FILE_LIST, REQUEST_FILE_LOCATION, REQUEST_CHUNK_REGISTER, REQUEST_LEAVE, \
    REPLY_REGISTER, REPLY_FILE_LIST, REPLY_PUBLISH, REPLY_FILE_LOCATION, REPLY_LEAVE, \
    PEER_GREET, PEER_REQUEST_CHUNK, PEER_REPLY_CHUNK = range(14)
