from p2pfs.core.server import MessageServer
from p2pfs.core.message import MessageType
import socket
import logging
import os.path
import threading
logger = logging.getLogger(__name__)


class Peer(MessageServer):
    def __init__(self, host, port, server, server_port):
        super().__init__(host, port)
        self._peers = {}
        # (remote filename) <-> (local filename)
        self._file_map = {}

        # locks and results for publish method
        self._publish_locks = {}
        self._publish_results = {}

        # locks and results for list_file method
        self._list_file_lock = threading.Lock()
        self._file_list = {}

        # socket connected to server
        try:
            self._server_sock = self._connect(server, server_port)
        except ConnectionRefusedError:
            logger.error('Server connection refused!')
            exit(1)

    def publish(self, file):
        # TODO: this method is not thread-safe, the following line only prevents sequential re-entrant
        # but 2 threads can both pass the condition check and the lock-twice trick will possibly cause a deadlock
        if file in self._publish_locks:
            return False, 'Publish file {} already in progress.'.format(file)
        if not os.path.exists(file):
            return False, 'File {} doesn\'t exist'.format(file)

        # send out the request packet
        path, filename = os.path.split(file)
        self._write_message(self._server_sock, {
            'type': MessageType.REQUEST_PUBLISH,
            'filename': filename,
            'size': os.stat(file).st_size
        })

        # we need to lock twice
        lock = threading.Lock()
        self._publish_locks[filename] = lock
        # first lock acquires the resource
        lock.acquire()
        # second lock blocks the method until reply is ready
        lock.acquire()
        # when we wake up it means the result is ready
        is_success, message = self._publish_results[filename]
        if is_success:
            self._file_map[filename] = file
            logger.info('File {} published on server with name {}'.format(file, filename))
        else:
            logger.info('File {} failed to publish, {}'.format(file, message))

        # remove the locks and results
        del self._publish_locks[filename]
        del self._publish_results[filename]
        # just one release here, the other release is called within _process_message method when
        # reply is ready
        lock.release()
        return is_success, message

    def list_file(self):
        # TODO: lock-twice trick is not thread-safe
        self._write_message(self._server_sock, {
            'type': MessageType.REQUEST_FILE_LIST,
        })
        # same technique of publish method
        self._list_file_lock.acquire()
        self._list_file_lock.acquire()
        self._list_file_lock.release()
        return self._file_list

    def download(self, file, destination, progress):
        if file not in self._file_list.keys():
            # refresh the file list
            self.list_file()
            # still not in list
            if file not in self._file_list.keys():
                return False, 'Requested file {} does not exist'.format(file)
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
            self._publish_results[message['filename']] = (message['result'], message['message'])
            self._publish_locks[message['filename']].release()
        elif message['type'] == MessageType.REPLY_FILE_LIST:
            self._file_list = message['file_list']
            self._list_file_lock.release()
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
