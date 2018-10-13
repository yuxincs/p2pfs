class DownloadIncompleteError(EOFError):
    def __init__(self, chunknum):
        self.chunknum = chunknum
