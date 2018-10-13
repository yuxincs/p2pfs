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
logger = logging.getLogger(__name__)


class DownloadManager:
    def __init__(self, tracker_reader, tracker_writer, filename, server_address, window_size):
        self._tracker_reader = tracker_reader
        self._tracker_writer = tracker_writer
        self._filename = filename
        self._server_address = server_address
        self._window_size = window_size

        self._file_chunk_info = None
        self._total_chunknum = -1

        # for disconnect recovery
        self._pending_chunknum = {}
        # to download queue
        self._to_download_chunk = None

        # peers and their read tasks
        # peer_address -> [reader, writer, RTT]
        self._peers = {}
        self._read_tasks = {}

        # indicating the tracker's connectivity
        self._is_connected = True

    async def _update_peer_rtt(self, addresses):
        """ Test multiple peer's rtt, must have registered in _peers, remove the registration if cannot connect"""
        # read_coro -> address
        read_coros = {}
        for address in addresses:
            reader, writer, _ = self._peers[address]
            try:
                # send out ping packet
                await write_message(writer, {
                    'type': MessageType.PEER_PING_PONG
                })
                # register read task
                read_coros[read_message(reader)] = address
                # set current time
                self._peers[address][2] = time.time()
            except ConnectionError:
                del self._peers[address]
        # start reading from peers to get pong packets
        # read_task -> address
        read_tasks = {asyncio.ensure_future(read_coro): address for read_coro, address in read_coros}
        for done in asyncio.as_completed(read_tasks):
            address = read_tasks[done]
            try:
                await done
                self._peers[address][2] = time.time() - self._peers[address][2]
            except asyncio.IncompleteReadError:
                del self._peers[address]

    async def _request_chunkinfo(self):
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

    async def update_chunkinfo(self, without=None):
        """ update internal chunkinfo, doesn't raise exceptions"""
        if not self._is_connected:
            return
        try:
            fileinfo, chunkinfo = await self._request_chunkinfo()
            self._total_chunknum = fileinfo['total_chunknum']
        except (asyncio.IncompleteReadError, ConnectionError):
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
            self._file_chunk_info = {chunknum: set() for chunknum in range(self._total_chunknum)}
            self._to_download_chunk = list(self._file_chunk_info.keys())
        else:
            # reset the chunk info
            self._file_chunk_info = {chunknum: set() for chunknum in self._file_chunk_info.keys()}
        # chunkinfo: {address -> possessed_chunks}
        for address, possessed_chunks in chunkinfo.items():
            if address == without:
                continue
            for chunknum in possessed_chunks:
                # if chunknum hasn't been successfully downloaded
                if chunknum in self._file_chunk_info:
                    self._file_chunk_info[chunknum].add(address)

        self._to_download_chunk.sort(key=lambda num: len(self._file_chunk_info[num]))

    async def download(self):
        # first update chunkinfo
        await self.update_chunkinfo()
        yield None, None

        # for disconnect recovery
        pending_chunknum = {}
        # initially schedule chunk requests of sliding window size
        for chunknum in range(min(self._window_size, total_chunknum)):
            if len(file_chunk_info[chunknum]) == 0:
                return False, 'File chunk #{} is not present on any peer.'.format(chunknum)

            fastest_peer = min(file_chunk_info[chunknum], key=lambda address: peer_rtts[address])
            await write_message(peers[fastest_peer][1], {
                'type': MessageType.PEER_REQUEST_CHUNK,
                'filename': file,
                'chunknum': chunknum
            })
            pending_chunknum[chunknum] = fastest_peer

        cursor = min(update_frequency, total_chunknum)

        read_tasks = {asyncio.ensure_future(self._read_message(reader)): peer_address
                      for peer_address, (reader, _) in peers.items()}
        try:
            with open(destination + '.temp', 'wb') as dest_file:
                self._file_map[file] = destination
                while len(file_chunk_info) != 0:
                    done, _ = await asyncio.wait(read_tasks.keys(), return_when=asyncio.FIRST_COMPLETED)
                    for finished_task in done:
                        peer_address = read_tasks[finished_task]
                        peer_reader, peer_writer = peers[peer_address]
                        # remove finished task from read_tasks to stop waiting for next iteration
                        del read_tasks[finished_task]
                        try:
                            message = finished_task.result()

                            number, data, digest = message['chunknum'], message['data'], message['digest']
                            raw_data = pybase64.b64decode(data.encode('utf-8'), validate=True)

                            # ask for re-transmission if data is corrupted
                            if Peer._HASH_FUNC(raw_data).hexdigest() != digest:
                                await self._write_message(peer_writer, {
                                    'type': MessageType.PEER_REQUEST_CHUNK,
                                    'filename': file,
                                    'chunknum': number
                                })
                                # since the task is done, schedule a new task to be run
                                read_tasks[asyncio.ensure_future(self._read_message(peer_reader))] = peer_address
                                continue

                            yield number, raw_data

                            # remove successfully-received chunk from pending and download plans
                            del file_chunk_info[number]
                            del pending_chunknum[number]

                            # send request chunk register to server
                            try:
                                await self._write_message(self._tracker_writer, {
                                    'type': MessageType.REQUEST_CHUNK_REGISTER,
                                    'filename': file,
                                    'chunknum': number
                                })
                            except (ConnectionResetError, RuntimeError, BrokenPipeError):
                                # stop querying tracker
                                assert not await self.is_connected()
                                pass

                            if reporthook:
                                reporthook(total_chunknum - len(file_chunk_info), Peer._CHUNK_SIZE, fileinfo['size'])
                            logger.debug('Got {}\'s chunk # {}'.format(file, number))

                            # send out request chunk
                            if cursor < total_chunknum:
                                if len(file_chunk_info[cursor]) == 0 and await self.is_connected():
                                    # update chunkinfo to see if new peers have registered and update downloading plan
                                    try:
                                        _, chunkinfo = await self._request_chunkinfo(file)

                                        for address, possessed_chunks in chunkinfo.items():
                                            # if new peer appeared in chunkinfo
                                            if address not in peers:
                                                reader, writer = \
                                                    await asyncio.open_connection(*json.loads(address))
                                                peers[address] = reader, writer
                                                peer_rtts[address] = await self._test_peer_rtt(
                                                    (address, reader, writer))
                                                # schedule the read tasks to wait for
                                                read_tasks[asyncio.ensure_future(self._read_message(reader))] = address
                                            # update file chunk info
                                            file_chunk_info = {number: set() for number in file_chunk_info.keys()}
                                            for number in possessed_chunks:
                                                if number in file_chunk_info:
                                                    file_chunk_info[number].add(address)
                                    except (ConnectionResetError, RuntimeError, BrokenPipeError):
                                        assert not self.is_connected()
                                        pass

                                if len(file_chunk_info[cursor]) == 0:
                                    return False, 'Chunk #{} doesn\'t exist on any peers.'.format(cursor)

                                fastest_peer = min(file_chunk_info[cursor], key=lambda address: peer_rtts[address])

                                await self._write_message(peers[fastest_peer][1], {
                                    'type': MessageType.PEER_REQUEST_CHUNK,
                                    'filename': file,
                                    'chunknum': cursor
                                })
                                pending_chunknum[cursor] = fastest_peer
                                cursor += 1

                            if (total_chunknum - len(file_chunk_info)) % update_frequency:
                                # TODO: update chunkinfo and peers
                                pass

                            # since the task is done, schedule a new task to be run
                            read_tasks[asyncio.ensure_future(self._read_message(peer_reader))] = peer_address

                        # peer disconnected during receive period
                        except (asyncio.IncompleteReadError, RuntimeError, ConnectionResetError, BrokenPipeError):
                            logger.warning('{} disconnected!'.format(peer_address))
                            if not peer_writer.is_closing():
                                peer_writer.close()

                            # remove the current peer's registration
                            for chunknum in file_chunk_info.keys():
                                file_chunk_info[chunknum].remove(peer_address)
                            del peer_rtts[peer_address]
                            del peers[peer_address]

                            # update chunkinfo to see if new peers have registered and update downloading plan
                            try:
                                _, chunkinfo = await self._request_chunkinfo(file)

                                for address, possessed_chunks in chunkinfo.items():
                                    # if the chunkinfo hasn't been updated with peer_address removed
                                    if address == peer_address:
                                        continue

                                    # if new peer appeared in chunkinfo
                                    if address not in peers:
                                        reader, writer = \
                                            await asyncio.open_connection(*json.loads(address))
                                        peers[address] = reader, writer
                                        peer_rtts[address] = await self._test_peer_rtt((address, reader, writer))
                                        # schedule the read tasks to wait for
                                        read_tasks[asyncio.ensure_future(self._read_message(reader))] = address
                                    # update file chunk info
                                    file_chunk_info = {number: set() for number in file_chunk_info.keys()}
                                    for number in possessed_chunks:
                                        if number in file_chunk_info and address != peer_address:
                                            file_chunk_info[number].add(address)
                            except ConnectionError:
                                pass

                            # if the disconnected peer has any pending chunks to receive
                            # request from other peers
                            for pending, registered_peer in pending_chunknum.items():
                                if peer_address == registered_peer:
                                    if len(file_chunk_info[pending]) == 0:
                                        return False, 'Chunk #{} doesn\'t exist on any peers.'.format(pending)
                                    fastest_peer = min(file_chunk_info[pending],
                                                       key=lambda address: peer_rtts[address])
                                    _, writer = peers[fastest_peer]
                                    await self._write_message(writer, {
                                        'type': MessageType.PEER_REQUEST_CHUNK,
                                        'filename': file,
                                        'chunknum': pending
                                    })
                                    pending_chunknum[pending] = fastest_peer

    async def clean(self):
        # cancel current reading tasks
        for task in self._read_tasks.keys():
            task.cancel()
        # close the connections
        for _, (_, writer) in self._peers.items():
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
            return False, 'Already connected!'
        # connect to server
        try:
            self._tracker_reader, self._tracker_writer = \
                await asyncio.open_connection(*tracker_address, loop=loop)
        except ConnectionRefusedError:
            logger.error('Server connection refused!')
            return False, 'Server connection refused!'
        # send out register message
        logger.info('Requesting to register')
        await write_message(self._tracker_writer, {
            'type': MessageType.REQUEST_REGISTER,
            'address': self._server_address
        })
        message = await read_message(self._tracker_reader)
        assert MessageType(message['type']) == MessageType.REPLY_REGISTER
        logger.info('Successfully registered.')
        return True, 'Connected!'

    async def disconnect(self):
        if not self._tracker_writer or self._tracker_writer.is_closing():
            return False, 'Already disconnected'
        self._tracker_writer.close()
        await self._tracker_writer.wait_closed()

        self._reset()
        return True, 'Disconnected!'

    def _reset(self):
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
            return False, 'File {} doesn\'t exist'.format(local_file)

        _, remote_name = os.path.split(local_file) if remote_name is None else remote_name

        if remote_name in self._pending_publish:
            return False, 'Publish file {} already in progress.'.format(local_file)

        if not await self.is_connected():
            return False, 'Not connected, try \'connect <tracker_ip> <tracker_port>\''

        self._pending_publish.add(remote_name)

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
        is_success, message = message['result'], message['message']

        if is_success:
            self._file_map[remote_name] = local_file
            logger.info('File {} published on server with name {}'.format(local_file, remote_name))
        else:
            logger.info('File {} failed to publish, {}'.format(local_file, message))

        self._pending_publish.remove(remote_name)
        return is_success, message

    async def list_file(self):
        if not await self.is_connected():
            return None, 'Not connected, try \'connect <tracker_ip> <tracker_port>\''
        await write_message(self._tracker_writer, {
            'type': MessageType.REQUEST_FILE_LIST,
        })
        message = await read_message(self._tracker_reader)
        assert MessageType(message['type']) == MessageType.REPLY_FILE_LIST
        return message['file_list'], 'Success'

    async def download(self, file, destination, reporthook=None):
        # request for file list
        file_list, _ = await self.list_file()
        if not file_list or file not in file_list:
            return False, 'Requested file {} does not exist.'.format(file)

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
        finally:
            await download_manager.clean()

            # change the temp file into the actual file
            os.rename(destination + '.temp', destination)

        return True, 'File {} dowloaded to {}'.format(file, destination)

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
            except (asyncio.IncompleteReadError, ConnectionError):
                break
