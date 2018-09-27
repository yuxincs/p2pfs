from p2pfs.core.server import MessageServer
from p2pfs.core.message import MessageType
import socket
import logging
import os.path
import threading
from queue import Queue
import math
import pybase64
logger = logging.getLogger(__name__)


class Peer(MessageServer):
    def __init__(self, host, port, server, server_port):
        super().__init__(host, port)
        self._peers = {}
        # (remote filename) <-> (local filename)
        self._file_map = {}

        # lock and results for publish method
        self._publish_lock = threading.Lock()
        self._publish_results = {}

        # lock and results for list_file method
        self._file_list = None
        self._file_list_lock = threading.Lock()
        self._file_list_result = Queue()

        # lock and results for download
        self._download_lock = threading.Lock()
        self._download_results = {}

        # socket connected to server
        try:
            self._server_sock = self._connect(server, server_port)
        except ConnectionRefusedError:
            logger.error('Server connection refused!')
            exit(1)

    def publish(self, file):
        path, filename = os.path.split(file)
        # guard the check to prevent 2 threads passing the check simultaneously
        with self._publish_lock:
            if filename in self._publish_results:
                return False, 'Publish file {} already in progress.'.format(file)
            self._publish_results[filename] = Queue(maxsize=1)
        if not os.path.exists(file):
            return False, 'File {} doesn\'t exist'.format(file)

        # send out the request packet
        self._write_message(self._server_sock, {
            'type': MessageType.REQUEST_PUBLISH,
            'filename': filename,
            'size': os.stat(file).st_size
        })

        # queue will block until the result is ready
        is_success, message = self._publish_results[filename].get()
        if is_success:
            self._file_map[filename] = file
            logger.info('File {} published on server with name {}'.format(file, filename))
        else:
            logger.info('File {} failed to publish, {}'.format(file, message))

        # remove result
        with self._publish_lock:
            del self._publish_results[filename]
        return is_success, message

    def list_file(self):
        self._write_message(self._server_sock, {
            'type': MessageType.REQUEST_FILE_LIST,
        })
        with self._file_list_lock:
            self._file_list = self._file_list_result.get()
        return self._file_list

    def download(self, file, destination, progress):
        with self._file_list_lock:
            if self._file_list is None or file not in self._file_list.keys():
                return False, 'Requested file {} does not exist, try list_file?'.format(file)
        with self._download_lock:
            if file in self._download_results:
                return False, 'Download {} already in progress.'.format(file)
            self._download_results[file] = Queue()

        self._write_message(self._server_sock, {
            'type': MessageType.REQUEST_FILE_LOCATION,
            'filename': file
        })
        # wait until reply is ready
        fileinfo, chunkinfo = self._download_results[file].get()
        totalchunknum = math.ceil(fileinfo['size'] / (512 * 1024))
        logger.debug('{}: {} ==> {}'.format(file, fileinfo, chunkinfo))

        # TODO: refactor this block, make it prettier
        for chunknum in range(totalchunknum):
            for peer_id, possessed_chunks in chunkinfo.items():
                # first one is a range
                smallest, largest = possessed_chunks[0]
                if smallest <= chunknum <= largest or chunknum in possessed_chunks:
                    # find the client based on peer id
                    peer = None
                    for client, client_id in self._peers.items():
                        if client_id == peer_id:
                            peer = client
                    # TODO: handle this later
                    assert peer is not None
                    # write the message to ask the chunk
                    self._write_message(peer, {
                        'type': MessageType.PEER_REQUEST_CHUNK,
                        'filename': file,
                        'chunknum': chunknum
                    })
                    break

        with open(destination, 'wb') as dest_file:
            dest_file.write(b'0' * fileinfo['size'])
            dest_file.flush()
            self._file_map[file] = destination
            for i in range(totalchunknum):
                number, raw_data = self._download_results[file].get()
                dest_file.seek(number * (512 * 1024), 0)
                dest_file.write(pybase64.b64decode(raw_data.encode('utf-8'), validate=True))
                dest_file.flush()
                progress(i + 1, totalchunknum)
                logger.debug('Got {}\'s chunk # {}'.format(file, number))

        with self._download_lock:
            del self._download_results[file]

        return True, 'File {} dowloaded to {}'.format(file, destination)

    def exit(self):
        self._server_sock.close()
        for client, _ in self._peers.items():
            client.close()

    def _server_started(self):
        logger.info('Requesting to register')
        self._write_message(self._server_sock, {
            'type': MessageType.REQUEST_REGISTER,
            'address': self._sock.getsockname()
        })

    def _client_connected(self, client):
        assert isinstance(client, socket.socket)
        self._peers[client] = None
        logger.debug(self._peers.values())

    def _process_message(self, client, message):
        if message['type'] == MessageType.REPLY_REGISTER:
            logger.info('Successfully registered with id {}'.format(message['id']))
            for (id, (ip, port)) in message['peer_list']:
                client = self._connect(ip, port)
                self._peers[client] = id
                self._write_message(client, {
                    'type': MessageType.PEER_GREET,
                    'id': message['id']
                })
        elif message['type'] == MessageType.PEER_GREET:
            logger.info('Greetings from peer with id {}'.format(message['id']))
            assert client in self._peers
            self._peers[client] = message['id']
        elif message['type'] == MessageType.REPLY_PUBLISH:
            self._publish_results[message['filename']].put((message['result'], message['message']))
        elif message['type'] == MessageType.REPLY_FILE_LIST:
            self._file_list_result.put(message['file_list'])
        elif message['type'] == MessageType.REPLY_FILE_LOCATION:
            self._download_results[message['filename']].put((message['fileinfo'], message['chunkinfo']))
        elif message['type'] == MessageType.PEER_REQUEST_CHUNK:
            assert message['filename'] in self._file_map, 'File {} requested does not exist'.format(message['filename'])
            local_file = self._file_map[message['filename']]
            with open(local_file, 'rb') as f:
                f.seek(message['chunknum'] * (512 * 1024), 0)
                raw_data = f.read(512 * 1024)
            self._write_message(client, {
                'type': MessageType.PEER_REPLY_CHUNK,
                'filename': message['filename'],
                'chunknum': message['chunknum'],
                'data': pybase64.b64encode(raw_data).decode('utf-8')
            })
            self._write_message(self._server_sock, {
                'type': MessageType.REQUEST_CHUNK_REGISTER,
                'filename': message['filename'],
                'chunknum': message['chunknum']
            })
        elif message['type'] == MessageType.PEER_REPLY_CHUNK:
            self._download_results[message['filename']].put((message['chunknum'], message['data']))
        else:
            logger.error('Undefined message with type {}, full message: {}'.format(message['type'], message))

        logger.debug(self._peers.values())

    def _client_closed(self, client):
        assert isinstance(client, socket.socket)
        if client is self._server_sock:
            logger.error('Server {} closed unexpectedly'.format(client.getpeername()))
            exit(1)
        else:
            del self._peers[client]
        logger.debug(self._peers.values())
