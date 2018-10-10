import logging
import os.path
import math
import json
import time
import hashlib
import asyncio
import pybase64
from p2pfs.core.server import MessageServer, MessageType
logger = logging.getLogger(__name__)


class Peer(MessageServer):
    _CHUNK_SIZE = 512 * 1024
    _HASH_FUNC = hashlib.sha256

    def __init__(self, host, port, server, server_port, loop=None):
        super().__init__(host, port, loop=loop)
        self._tracker_address = (server, server_port)
        self._tracker_reader, self._tracker_writer = None, None

        # (remote filename) <-> (local filename)
        self._file_map = {}

        self._pending_publish = set()

        self._delay = 0

    async def start(self):
        # connect to server
        try:
            self._tracker_reader, self._tracker_writer = \
                await asyncio.open_connection(*self._tracker_address, loop=self._loop)
        except ConnectionRefusedError:
            logger.error('Server connection refused!')
            return False
        # start the internal server
        await super().start()
        # send out register message
        logger.info('Requesting to register')
        await self._write_message(self._tracker_writer, {
            'type': MessageType.REQUEST_REGISTER,
            'address': self._server_address
        })
        message = await self._read_message(self._tracker_reader)
        assert MessageType(message['type']) == MessageType.REPLY_REGISTER
        logger.info('Successfully registered.')
        return True

    async def stop(self):
        await super().stop()

        if not self._tracker_writer.is_closing():
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

        self._pending_publish.add(remote_name)

        # send out the request packet
        await self._write_message(self._tracker_writer, {
            'type': MessageType.REQUEST_PUBLISH,
            'filename': remote_name,
            'fileinfo': {
                'size': os.stat(local_file).st_size,
                'total_chunknum': math.ceil(os.stat(local_file).st_size / Peer._CHUNK_SIZE)
            },
        })

        message = await self._read_message(self._tracker_reader)
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
        await self._write_message(self._tracker_writer, {
            'type': MessageType.REQUEST_FILE_LIST,
        })
        message = await self._read_message(self._tracker_reader)
        assert MessageType(message['type']) == MessageType.REPLY_FILE_LIST
        return message['file_list']

    async def _request_chunkinfo(self, filename):
        await self._write_message(self._tracker_writer, {
            'type': MessageType.REQUEST_FILE_LOCATION,
            'filename': filename
        })

        message = await self._read_message(self._tracker_reader)
        assert MessageType(message['type']) == MessageType.REPLY_FILE_LOCATION
        fileinfo, chunkinfo = message['fileinfo'], message['chunkinfo']
        logger.debug('{}: {} ==> {}'.format(filename, fileinfo, chunkinfo))
        return fileinfo, chunkinfo

    async def _test_peer_rtt(self, peers):
        """
        :param peers: {peer_address -> (reader, writer)} or (peer_address, reader, writer)
        :return: {peer_address -> rtt} or rtt
        """
        result = {} if isinstance(peers, dict) else -1
        if isinstance(peers, dict):
            for peer_address, (_, writer) in peers.items():
                # send out ping packet
                await self._write_message(writer, {
                    'type': MessageType.PEER_PING_PONG,
                    'peer_address': peer_address
                })
                # set current time
                result[peer_address] = time.time()
            # start reading from peers to get pong packets
            for done in asyncio.as_completed(
                    {asyncio.ensure_future(self._read_message(reader)) for (reader, _) in peers.values()}):
                message = await done
                peer_address = message['peer_address']
                result[peer_address] = time.time() - result[peer_address]
        else:
            peer_address, reader, writer = peers
            result = time.time()
            await self._write_message(writer, {
                'type': MessageType.PEER_PING_PONG,
                'peer_address': peer_address
            })
            await self._read_message(reader)
            result = time.time() - result

        return result

    async def download(self, file, destination, reporthook=None):
        # request for file list
        file_list = await self.list_file()
        if file not in file_list:
            return False, 'Requested file {} does not exist.'.format(file)

        fileinfo, chunkinfo = await self._request_chunkinfo(file)

        total_chunknum = fileinfo['total_chunknum']

        # peer_address -> (reader, writer)
        peers = {}
        # connect to all peers and do a speed test
        for peer_address in chunkinfo.keys():
            # peer_address is a string, since JSON requires keys being strings
            peers[peer_address] = await asyncio.open_connection(*json.loads(peer_address), loop=self._loop)

        peer_rtts = await self._test_peer_rtt(peers)

        # setup initial download plan
        file_chunk_info = {chunknum: set() for chunknum in range(total_chunknum)}
        for peer_address, peer_chunks in chunkinfo.items():
            for chunknum in peer_chunks:
                file_chunk_info[chunknum].add(peer_address)

        # update chunkinfo every UPDATE_FREQUENCY chunks
        update_frequency = 30

        # for disconnect recovery
        pending_chunknum = {}
        # schedule UPDATE_FREQUENCY chunk requests
        for chunknum in range(min(update_frequency, total_chunknum)):
            if len(file_chunk_info[chunknum]) == 0:
                return False, 'File chunk #{} is not present on any peer.'.format(chunknum)

            asyncio.ensure_future(self._write_message(peers[fastest_peer][1], {
            fastest_peer = min(file_chunk_info[chunknum], key=lambda address: peer_rtts[address])
                'type': MessageType.PEER_REQUEST_CHUNK,
                'filename': file,
                'chunknum': chunknum
            }))
            pending_chunknum[chunknum] = fastest_peer

        cursor = min(update_frequency, total_chunknum)

        read_tasks = {asyncio.ensure_future(self._read_message(reader)): peer_address
                      for peer_address, (reader, _) in peers.items()}
        try:
            with open(destination + '.temp', 'wb') as dest_file:
                self._file_map[file] = destination
                while len(file_chunk_info) != 0:
                    done, _ = await asyncio.wait(read_tasks.keys(), return_when=asyncio.FIRST_COMPLETED)
                    for task in done:
                        try:
                            message = task.result()

                            # since the task is done, schedule a new task to be run
                            peer_address = read_tasks[task]
                            reader, writer = peers[peer_address]
                            read_tasks[asyncio.ensure_future(self._read_message(reader))] = peer_address
                            del read_tasks[task]

                            number, data, digest = message['chunknum'], message['data'], message['digest']
                            raw_data = pybase64.b64decode(data.encode('utf-8'), validate=True)

                            # ask for re-transmission if data is corrupted
                            if Peer._HASH_FUNC(raw_data).hexdigest() != digest:
                                asyncio.ensure_future(self._write_message(writer, {
                                    'type': MessageType.PEER_REQUEST_CHUNK,
                                    'filename': file,
                                    'chunknum': number
                                }))

                            dest_file.seek(number * Peer._CHUNK_SIZE, 0)
                            dest_file.write(raw_data)
                            dest_file.flush()

                            # remove successfully-received chunk from pending and download plans
                            del file_chunk_info[number]

                            # send request chunk register to server
                            await self._write_message(self._tracker_writer, {
                                'type': MessageType.REQUEST_CHUNK_REGISTER,
                                'filename': file,
                                'chunknum': number
                            })
                            if reporthook:
                                reporthook(total_chunknum - len(file_chunk_info), Peer._CHUNK_SIZE, fileinfo['size'])
                            logger.debug('Got {}\'s chunk # {}'.format(file, number))

                            # send out request chunk
                            if cursor < total_chunknum:
                                if len(file_chunk_info[cursor]) == 0:
                                    return False, 'File chunk #{} is not present on any peer.'.format(cursor)

                                fastest_peer = min(file_chunk_info[cursor], key=lambda address: peer_rtts[address])

                                asyncio.ensure_future(self._write_message(peers[fastest_peer][1], {
                                    'type': MessageType.PEER_REQUEST_CHUNK,
                                    'filename': file,
                                    'chunknum': cursor
                                }))
                                cursor += 1

                            if (total_chunknum - len(file_chunk_info)) % update_frequency:
                                # TODO: update chunkinfo and peers
                                pass

                        except asyncio.IncompleteReadError:
                            # TODO: update chunkinfo and peers
                            del read_tasks[task]
        finally:
            # cancel current reading tasks
            for task in read_tasks.keys():
                task.cancel()
            # close the connections
            for _, (_, writer) in peers.items():
                if not writer.is_closing():
                    writer.close()
                    await writer.wait_closed()

            # change the temp file into the actual file
            os.rename(destination + '.temp', destination)

        return True, 'File {} dowloaded to {}'.format(file, destination)

    async def _process_connection(self, reader, writer):
        assert isinstance(reader, asyncio.StreamReader) and isinstance(writer, asyncio.StreamWriter)
        while not reader.at_eof():
            try:
                message = await self._read_message(reader)
            except asyncio.IncompleteReadError:
                break
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
                await self._write_message(writer, {
                    'type': MessageType.PEER_REPLY_CHUNK,
                    'filename': message['filename'],
                    'chunknum': message['chunknum'],
                    'data': pybase64.b64encode(raw_data).decode('utf-8'),
                    'digest': Peer._HASH_FUNC(raw_data).hexdigest()
                })
            elif message_type == MessageType.PEER_PING_PONG:
                await self._write_message(writer, message)
            else:
                logger.error('Undefined message: {}'.format(self._message_log(message)))
