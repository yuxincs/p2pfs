class DownloadIncompleteError(EOFError):
    def __init__(self, message, chunknum):
        super().__init__(message)
        self.chunknum = chunknum


class AlreadyConnectedError(ConnectionError):
    def __init__(self, address):
        self.address = address


class TrackerNotConnectedError(ConnectionError):
    pass


class InProgressError(Exception):
    pass


class ServerRunningError(Exception):
    pass
