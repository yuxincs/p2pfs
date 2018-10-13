from abc import abstractmethod
import logging
import asyncio
from p2pfs.core.exceptions import ServerRunningError
logger = logging.getLogger(__name__)


class MessageServer:
    """ Base class for async TCP server, provides basic start and stop methods.
    """
    _SOCKET_TIMEOUT = 5

    def __init__(self, ):
        # internal server states
        self._is_running = False
        self._server_address = None

        # manage the connections
        self._writers = set()
        self._server = None

    def is_running(self):
        return self._is_running

    async def start(self, local_address, loop=None):
        if self._is_running:
            raise ServerRunningError()
        logger.info('Start listening on {}'.format(local_address))
        # start server
        self._server = await asyncio.start_server(self.__new_connection, *local_address, loop=loop)
        # update server address, only get the first 2 elements because under IPv6 the return value contains 4 elements
        # see https://docs.python.org/3.7/library/socket.html#socket-families
        self._server_address = self._server.sockets[0].getsockname()[:2]
        self._is_running = True
        return True

    async def stop(self):
        if self._is_running:
            logger.warning('Shutting down {}'.format(self))
            self._is_running = False
            self._server.close()
            await self._server.wait_closed()
            for writer in set(self._writers):
                if not writer.is_closing():
                    writer.close()
                    await writer.wait_closed()

            if len(self._writers) != 0:
                logger.warning('Writers not fully cleared {}'.format(self._writers))

            self._writers = set()

    async def __new_connection(self, reader, writer):
        self._writers.add(writer)
        try:
            await self._process_connection(reader, writer)
        finally:
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()
            self._writers.remove(writer)

    @abstractmethod
    async def _process_connection(self, reader, writer):
        raise NotImplementedError
