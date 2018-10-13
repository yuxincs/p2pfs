import logging
import os.path
import math
import json
import time
import hashlib
import asyncio
import pybase64
from p2pfs.core.message import MessageType, read_message, write_message
from p2pfs.core.server import MessageServer
from p2pfs.core.exceptions import *
logger = logging.getLogger(__name__)


class DownloadManager:
    def __init__(self, tracker_reader, tracker_writer, filename, server_address, window_size):
        self._tracker_reader = tracker_reader
        self._tracker_writer = tracker_writer
        self._filename = filename
        self._server_address = server_address
        self._window_size = window_size

        self._file_chunk_info = None
        self._fileinfo = None

        # for disconnect recovery
        self._pending_chunknum = {}
        # to download queue
        self._to_download_chunk = None

        # peer_address -> [reader, writer, RTT]
        self._peers = {}
        self._read_tasks = {}

        # indicating the tracker's connectivity
        self._is_connected = True

    async def _update_peer_rtt(self, addresses):
        """ Test multiple peer's rtt, must have registered in _peers, doesn't raise exceptions"""
        # read_coro -> address
        read_tasks = set()
        for address in addresses:
            reader, writer, _ = self._peers[address]
            try:
                # send out ping packet
                await write_message(writer, {
                    'type': MessageType.PEER_PING_PONG,
                    'peer_address': address
                })
                # register read task
                read_tasks.add(asyncio.ensure_future(read_message(reader)))
                # set current time
                self._peers[address][2] = time.time()
            except (ConnectionError, RuntimeError):
                # if cannot send ping pong packet to peer, the rtt remains math.inf
                # won't cause trouble
                pass
        # start reading from peers to get pong packets
        for done in asyncio.as_completed(read_tasks):
            try:
                message = await done
                address = message['peer_address']
                self._peers[address][2] = time.time() - self._peers[address][2]
            except asyncio.IncompleteReadError:
                # Trick: if cannot read ping pong packet from peer, the rtt should reset to math.inf
                # however, since time.time() is relatively large enough to be similar to math.inf
                # it won't cause trouble
                pass
        # Note: we will hide exceptions here since the exceptions will re-arise
        # when we do read task in download main body and we will handle the exceptions there

    async def _request_chunkinfo(self):
        """ send request chunkinfo to tracker, will raise exceptions """
        await write_message(self._tracker_writer, {
            'type': MessageType.REQUEST_FILE_LOCATION,
            'filename': self._filename
        })

        message = await read_message(self._tracker_reader)
        assert MessageType(message['type']) == MessageType.REPLY_FILE_LOCATION
        fileinfo, chunkinfo = message['fileinfo'], message['chunkinfo']
        logger.debug('{}: {} ==> {}'.format(self._filename, fileinfo, chunkinfo))
        # cancel out self registration
        if json.dumps(self._server_address) in chunkinfo:
            del chunkinfo[json.dumps(self._server_address)]
        return fileinfo, chunkinfo

    async def update_chunkinfo(self, exclude=None):
        """ update internal chunkinfo, doesn't raise exceptions"""
        if not self._is_connected:
            return
        try:
            self._fileinfo, chunkinfo = await self._request_chunkinfo()
        except (asyncio.IncompleteReadError, ConnectionError, RuntimeError):
            # if tracker is down
            self._is_connected = False
            return

        to_update_rtts = set()
        # peer_address -> (reader, writer)
        # connect to all peers and do a speed test
        for address in chunkinfo.keys():
            # peer_address is a string, since JSON requires keys being strings
            if address not in self._peers:
                try:
                    reader, writer = await asyncio.open_connection(*json.loads(address))
                    self._peers[address] = [reader, writer, math.inf]
                    to_update_rtts.add(address)
                except ConnectionRefusedError:
                    continue

        await self._update_peer_rtt(to_update_rtts)

        # update file chunk info
        if not self._file_chunk_info:
            # initialize if never initialized
            self._file_chunk_info = {chunknum: set() for chunknum in range(self._fileinfo['total_chunknum'])}
            self._to_download_chunk = list(self._file_chunk_info.keys())
        else:
            # reset the chunk info
            self._file_chunk_info = {chunknum: set() for chunknum in self._file_chunk_info.keys()}
        # chunkinfo: {address -> possessed_chunks}
        for address, possessed_chunks in chunkinfo.items():
            if address == exclude:
                continue
            for chunknum in possessed_chunks:
                # if chunknum hasn't been successfully downloaded
                if chunknum in self._file_chunk_info:
                    self._file_chunk_info[chunknum].add(address)

        # sort the to-download queue based on rareness
        self._to_download_chunk.sort(key=lambda num: len(self._file_chunk_info[num]))

    async def _send_request_chunk(self, chunknum):
        if len(self._file_chunk_info[chunknum]) == 0:
            raise DownloadIncompleteError(message='Download cannot proceed.', chunknum=chunknum)
        fastest_peer = min(self._file_chunk_info[chunknum], key=lambda address: self._peers[address][2])
        try:
            await write_message(self._peers[fastest_peer][1], {
                'type': MessageType.PEER_REQUEST_CHUNK,
                'filename': self._filename,
                'chunknum': chunknum
            })
        except ConnectionError:
            # if write task fails, the error will eventually reflect on the read task
            # we'll handle the exception there
            pass
        self._pending_chunknum[chunknum] = fastest_peer

    def get_progress(self):
        """ Returns finished_chunknum, total_file_size """
        return self._fileinfo['total_chunknum'] - len(self._file_chunk_info), self._fileinfo['size']

    async def download(self):
        # first update chunkinfo
        await self.update_chunkinfo()

        # works as follows:
        # to_download is a list of chunknum sorted based on rareness, re-sorted everytime by update_chunk
        # file_chunk_info contains what peers possess what chunk
        # pending_chunknum is a set which we have sent out request but haven't received chunk data yet, this is
        # needed for peer disconnect recovery

        # 1. send out initial windows_size requests packet from to_download (pop off the number)
        # 2. put the (chunknum, peer) tuple into pending_chunknum set
        # 3. everytime a chunk data has arrived, remove the chunknum from pending_chunknum and file_chunk_info
        # 4. if any peer disconnects, remove it from _peers, check if we have sent requests to it
        #    (using pending_chunk_num), send the same request to alternative peers.
        # 5. if there's a chunknum which no peers possess, raise asyncio.IncompleteReadError

        # initially schedule chunk requests of sliding window size
        for _ in range(min(self._window_size, self._fileinfo['total_chunknum'])):
            chunknum = self._to_download_chunk.pop(0)
            await self._send_request_chunk(chunknum)

        self._read_tasks = {asyncio.ensure_future(read_message(reader)): address
                            for address, (reader, _, _) in self._peers.items()}

        while len(self._file_chunk_info) != 0:
            done, _ = await asyncio.wait(self._read_tasks.keys(), return_when=asyncio.FIRST_COMPLETED)
            for finished_task in done:
                # remove finished task from read_tasks to stop waiting for next iteration
                peer_address = self._read_tasks.pop(finished_task)
                reader, writer, _ = self._peers[peer_address]

                try:
                    message = finished_task.result()

                    number, data, digest = message['chunknum'], message['data'], message['digest']
                    raw_data = pybase64.b64decode(data.encode('utf-8'), validate=True)

                    # ask for re-transmission if data is corrupted
                    if Peer._HASH_FUNC(raw_data).hexdigest() != digest:
                        await write_message(writer, {
                            'type': MessageType.PEER_REQUEST_CHUNK,
                            'filename': self._filename,
                            'chunknum': number
                        })
                        # since the task is done, schedule a new task to be run
                        self._read_tasks[asyncio.ensure_future(read_message(reader))] = peer_address
                        continue

                    yield number, raw_data

                    # remove successfully-received chunk from pending and download plans
                    del self._file_chunk_info[number]
                    del self._pending_chunknum[number]

                    # send request chunk register to server
                    try:
                        await write_message(self._tracker_writer, {
                            'type': MessageType.REQUEST_CHUNK_REGISTER,
                            'filename': self._filename,
                            'chunknum': number
                        })
                    except (ConnectionError, RuntimeError):
                        # stop querying tracker
                        pass

                    # send out request chunk
                    if len(self._to_download_chunk) > 0:
                        chunknum = self._to_download_chunk.pop(0)
                        if self._file_chunk_info[chunknum] == 0:
                            await self.update_chunkinfo()
                        await self._send_request_chunk(chunknum)

                    # since the task is done, schedule a new task to be run
                    self._read_tasks[asyncio.ensure_future(read_message(reader))] = peer_address

                # peer disconnected during receive period
                except (asyncio.IncompleteReadError, ConnectionError):
                    logger.warning('{} disconnected!'.format(peer_address))

                    if not writer.is_closing():
                        writer.close()
                        await writer.wait_closed()

                    del self._peers[peer_address]

                    await self.update_chunkinfo(exclude=peer_address)

                    # if the disconnected peer has any pending chunks to receive
                    # request from other peers
                    for pending, registered_peer in self._pending_chunknum.items():
                        if peer_address == registered_peer:
                            await self._send_request_chunk(pending)

    async def clean(self):
        # cancel current reading tasks
        for task in self._read_tasks.keys():
            task.cancel()
        # close the connections
        for _, (_, writer, _) in self._peers.items():
            if not writer.is_closing():
                writer.close()
                await writer.wait_closed()


class Peer(MessageServer):
    _CHUNK_SIZE = 512 * 1024
    _HASH_FUNC = hashlib.sha256

    def __init__(self, ):
        super().__init__()
        self._tracker_reader, self._tracker_writer = None, None

        # (remote filename) <-> (local filename)
        self._file_map = {}

        self._pending_publish = set()

        self._delay = 0

    async def is_connected(self):
        if not self._tracker_writer:
            return False
        # tracker gracefully closed connection
        can_read = not self._tracker_reader.at_eof()
        can_write = True
        # tracker disconnects suddenly
        try:
            await self._tracker_writer.drain()
        except ConnectionError:
            can_write = False
            if not self._tracker_writer.is_closing():
                self._tracker_writer.close()
        is_connected = can_read and can_write

        return is_connected

    async def connect(self, tracker_address, loop=None):
        if await self.is_connected():
            raise AlreadyConnectedError(address=self._tracker_writer.get_extra_info('peername'))
        # connect to server
        self._tracker_reader, self._tracker_writer = \
            await asyncio.open_connection(*tracker_address, loop=loop)
        try:
            # send out register message
            logger.info('Requesting to register')
            await write_message(self._tracker_writer, {
                'type': MessageType.REQUEST_REGISTER,
                'address': self._server_address
            })
            message = await read_message(self._tracker_reader)
            assert MessageType(message['type']) == MessageType.REPLY_REGISTER
        except (ConnectionError, RuntimeError, asyncio.IncompleteReadError):
            logger.warning('Error occurred during communications with tracker.')
            if not self._tracker_writer.is_closing():
                self._tracker_writer.close()
                await self._tracker_writer.wait_closed()
            raise
        logger.info('Successfully registered.')

    async def disconnect(self):
        if not self._tracker_writer.is_closing():
            self._tracker_writer.close()
            await self._tracker_writer.wait_closed()
        # reset variables
        self._pending_publish = set()
        self._delay = 0
        self._file_map = {}

    async def stop(self):
        await super().stop()

        if await self.is_connected() and not self._tracker_writer.is_closing():
            self._tracker_writer.close()
            await self._tracker_writer.wait_closed()

    def set_delay(self, delay):
        self._delay = 0 if delay is None else delay

    async def publish(self, local_file, remote_name=None):
        if not os.path.exists(local_file):
            raise FileNotFoundError()

        _, remote_name = os.path.split(local_file) if remote_name is None else remote_name

        if remote_name in self._pending_publish:
            raise InProgressError()

        if not await self.is_connected():
            raise TrackerNotConnectedError()

        self._pending_publish.add(remote_name)
        try:
            # send out the request packet
            await write_message(self._tracker_writer, {
                'type': MessageType.REQUEST_PUBLISH,
                'filename': remote_name,
                'fileinfo': {
                    'size': os.stat(local_file).st_size,
                    'total_chunknum': math.ceil(os.stat(local_file).st_size / Peer._CHUNK_SIZE)
                },
            })
            message = await read_message(self._tracker_reader)
            assert MessageType(message['type']) == MessageType.REPLY_PUBLISH
            is_success = message['result']

            if is_success:
                self._file_map[remote_name] = local_file
                logger.info('File {} published on server with name {}'.format(local_file, remote_name))
            else:
                logger.info('File {} failed to publish, {}'.format(local_file, message))
                raise FileExistsError()
        except (ConnectionError, RuntimeError, asyncio.IncompleteReadError):
            logger.warning('Error occured during communications with tracker.')
            raise
        finally:
            self._pending_publish.remove(remote_name)

    async def list_file(self):
        if not await self.is_connected():
            raise TrackerNotConnectedError()
        try:
            await write_message(self._tracker_writer, {
                'type': MessageType.REQUEST_FILE_LIST,
            })
            message = await read_message(self._tracker_reader)
            assert MessageType(message['type']) == MessageType.REPLY_FILE_LIST
            return message['file_list']
        except (asyncio.IncompleteReadError, ConnectionError, RuntimeError):
            logger.warning('Error occured during communications with tracker.')
            if not self._tracker_writer.is_closing():
                self._tracker_writer.close()
                await self._tracker_writer.wait_closed()
            raise

    async def download(self, file, destination, reporthook=None):
        # request for file list
        file_list = await self.list_file()

        if not file_list or file not in file_list:
            raise FileNotFoundError()

        download_manager = DownloadManager(self._tracker_reader, self._tracker_writer, file,
                                           server_address=self._server_address, window_size=30)

        # update chunkinfo every UPDATE_FREQUENCY chunks
        update_frequency = 30

        try:
            with open(destination + '.temp', 'wb') as dest_file:
                self._file_map[file] = destination
                async for chunknum, data in download_manager.download():
                    dest_file.seek(chunknum * Peer._CHUNK_SIZE, 0)
                    dest_file.write(data)
                    dest_file.flush()
                if reporthook:
                    finished_chunknum, file_size = download_manager.get_progress()
                    reporthook(finished_chunknum, Peer._CHUNK_SIZE, file_size)
        finally:
            await download_manager.clean()

            # change the temp file into the actual file
            os.rename(destination + '.temp', destination)

        return

    async def _process_connection(self, reader, writer):
        assert isinstance(reader, asyncio.StreamReader) and isinstance(writer, asyncio.StreamWriter)
        while not reader.at_eof():
            # peer's server is stateless, no need to worry about peer disconnection
            # that's the problem of the other side
            try:
                message = await read_message(reader)
                # artificial delay for peer
                if self._delay != 0:
                    await asyncio.sleep(self._delay)
                message_type = MessageType(message['type'])
                if message_type == MessageType.PEER_REQUEST_CHUNK:
                    assert message['filename'] in self._file_map, 'File {} requested does not exist'.format(message['filename'])
                    local_file = self._file_map[message['filename']]
                    with open(local_file, 'rb') as f:
                        f.seek(message['chunknum'] * Peer._CHUNK_SIZE, 0)
                        raw_data = f.read(Peer._CHUNK_SIZE)
                    await write_message(writer, {
                        'type': MessageType.PEER_REPLY_CHUNK,
                        'filename': message['filename'],
                        'chunknum': message['chunknum'],
                        'data': pybase64.b64encode(raw_data).decode('utf-8'),
                        'digest': Peer._HASH_FUNC(raw_data).hexdigest()
                    })
                elif message_type == MessageType.PEER_PING_PONG:
                    await write_message(writer, message)
                else:
                    logger.error('Undefined message: {}'.format(message))
            except (asyncio.IncompleteReadError, ConnectionError, RuntimeError):
                break
