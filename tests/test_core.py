import os
import asyncio
import pytest
from p2pfs import Peer, Tracker
from tests.conftest import fmd5, TEST_SMALL_FILE, TEST_LARGE_FILE, TEST_SMALL_FILE_SIZE, TEST_LARGE_FILE_SIZE

pytestmark = pytest.mark.asyncio


async def test_server_refused(unused_tcp_port):
    peer = Peer('localhost', 0, 'localhost', unused_tcp_port)
    started = await peer.start()
    assert not started


async def test_start_stop(unused_tcp_port):
    tracker = Tracker('localhost', unused_tcp_port)
    peers = tuple(Peer('localhost', 0, 'localhost', unused_tcp_port) for _ in range(3))
    tracker_started = await tracker.start()
    # spawn peers concurrently
    peers_started = await asyncio.gather(*[peer.start() for peer in peers])
    assert tracker_started and all(peers_started)

    await tracker.stop()
    await asyncio.gather(*[peer.stop() for peer in peers])


async def test_publish_refuse(unused_tcp_port):
    tracker = Tracker('localhost', unused_tcp_port)
    peers = tuple(Peer('localhost', 0, 'localhost', unused_tcp_port) for _ in range(3))
    tracker_started = await tracker.start()
    # spawn peers concurrently
    peers_started = await asyncio.gather(*[peer.start() for peer in peers])
    assert tracker_started and all(peers_started)
    with open('test_publish_refuse', 'wb') as fout:
        fout.write(os.urandom(100))
    is_success, _ = await peers[0].publish('test_publish_refuse')
    assert is_success
    is_success, _ = await peers[0].publish('test_publish_refuse')
    assert not is_success
    os.remove('test_publish_refuse')
    await tracker.stop()
    await asyncio.gather(*[peer.stop() for peer in peers])


async def test_publish(unused_tcp_port):
    tracker = Tracker('localhost', unused_tcp_port)
    peers = tuple(Peer('localhost', 0, 'localhost', unused_tcp_port) for _ in range(2))
    tracker_started = await tracker.start()
    peer_started = await asyncio.gather(*[peer.start() for peer in peers])
    assert tracker_started and peer_started

    # peer0 publishes a small_file and peer1 publishes a large file
    is_success, _ = await peers[0].publish(TEST_SMALL_FILE)
    assert is_success
    file_list = tracker.file_list()
    assert TEST_SMALL_FILE in file_list
    assert file_list[TEST_SMALL_FILE]['size'] == TEST_SMALL_FILE_SIZE
    file_list = await peers[1].list_file()
    assert TEST_SMALL_FILE in file_list

    is_success, _ = await peers[1].publish(TEST_LARGE_FILE)
    assert is_success
    file_list = tracker.file_list()
    assert TEST_LARGE_FILE in file_list and TEST_SMALL_FILE in file_list
    assert file_list[TEST_LARGE_FILE]['size'] == TEST_LARGE_FILE_SIZE
    file_list = await peers[0].list_file()
    assert TEST_LARGE_FILE in file_list and TEST_SMALL_FILE in file_list
    await tracker.stop()
    await asyncio.gather(*[peer.stop() for peer in peers])


async def test_download(unused_tcp_port):
    tracker = Tracker('localhost', unused_tcp_port)
    peers = tuple(Peer('localhost', 0, 'localhost', unused_tcp_port) for _ in range(3))
    tracker_started = await tracker.start()
    peer_started = await asyncio.gather(*[peer.start() for peer in peers])
    assert tracker_started and peer_started

    # peer0 publishes a small_file and peer1 publishes a large file
    is_success, _ = await peers[0].publish(TEST_SMALL_FILE)
    assert is_success
    file_list = tracker.file_list()
    assert TEST_SMALL_FILE in file_list
    assert file_list[TEST_SMALL_FILE]['size'] == TEST_SMALL_FILE_SIZE
    file_list = await peers[1].list_file()
    assert TEST_SMALL_FILE in file_list

    is_success, _ = await peers[1].publish(TEST_LARGE_FILE)
    assert is_success
    file_list = tracker.file_list()
    assert TEST_LARGE_FILE in file_list and TEST_SMALL_FILE in file_list
    assert file_list[TEST_LARGE_FILE]['size'] == TEST_LARGE_FILE_SIZE
    file_list = await peers[0].list_file()
    assert TEST_LARGE_FILE in file_list and TEST_SMALL_FILE in file_list

    def reporthook(chunk_num, chunk_size, total_size):
        reporthook.value = (chunk_num, total_size)

    # download small file
    result, msg = await peers[1].download(TEST_SMALL_FILE, 'downloaded_' + TEST_SMALL_FILE, reporthook=reporthook)
    assert result is True
    assert os.path.exists('downloaded_' + TEST_SMALL_FILE)
    assert fmd5(TEST_SMALL_FILE) == fmd5('downloaded_' + TEST_SMALL_FILE)
    assert reporthook.value == (1, 1000)
    os.remove('downloaded_' + TEST_SMALL_FILE)

    # download large file from single source
    result, msg = await peers[0].download(TEST_LARGE_FILE, 'downloaded_' + TEST_LARGE_FILE + '_0')
    assert result is True
    assert os.path.exists('downloaded_' + TEST_LARGE_FILE + '_0')
    assert fmd5(TEST_LARGE_FILE) == fmd5('downloaded_' + TEST_LARGE_FILE + '_0')

    # download large file from multiple sources
    result, msg = await peers[2].download(TEST_LARGE_FILE, 'downloaded_' + TEST_LARGE_FILE + '_2')
    assert result is True
    assert os.path.exists('downloaded_' + TEST_LARGE_FILE + '_2')
    assert fmd5(TEST_LARGE_FILE) == fmd5('downloaded_' + TEST_LARGE_FILE + '_2')
    os.remove('downloaded_' + TEST_LARGE_FILE + '_0')
    os.remove('downloaded_' + TEST_LARGE_FILE + '_2')

    await tracker.stop()
    await asyncio.gather(*[peer.stop() for peer in peers])


async def test_peer_disconnect(unused_tcp_port):
    tracker = Tracker('localhost', unused_tcp_port)
    peer = Peer('localhost', 0, 'localhost', unused_tcp_port)
    tracker_started = await tracker.start()
    peer_started = await peer.start()
    assert tracker_started and peer_started

    is_suceess, _ = await peer.publish(TEST_SMALL_FILE)
    assert is_suceess
    assert TEST_SMALL_FILE in tracker.file_list()

    # stop peer and check the file has been removed
    await peer.stop()
    # return control to the loop for tracker code to run
    await asyncio.sleep(0)
    assert TEST_SMALL_FILE not in tracker.file_list()

    await tracker.stop()
