import os
import asyncio
import pytest
from p2pfs import Peer, Tracker
import time
from tests.conftest import fmd5, TEST_SMALL_FILE, TEST_LARGE_FILE, \
    TEST_SMALL_FILE_SIZE, TEST_LARGE_FILE_SIZE, TEST_SMALL_FILE_1
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
    peers = tuple(Peer('localhost', 0, 'localhost', unused_tcp_port) for _ in range(5))
    tracker_started = await tracker.start()
    peer_started = await asyncio.gather(*[peer.start() for peer in peers])
    assert tracker_started and peer_started
    try:
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
        is_success, _ = await peers[1].download(TEST_SMALL_FILE, 'downloaded_' + TEST_SMALL_FILE, reporthook=reporthook)
        assert os.path.exists('downloaded_' + TEST_SMALL_FILE)
        assert is_success
        assert fmd5(TEST_SMALL_FILE) == fmd5('downloaded_' + TEST_SMALL_FILE)
        assert reporthook.value == (1, 1000)

        # download large file from single source
        is_success, _ = await peers[0].download(TEST_LARGE_FILE, 'downloaded_' + TEST_LARGE_FILE + '_0')
        assert os.path.exists('downloaded_' + TEST_LARGE_FILE + '_0')
        assert is_success
        assert fmd5(TEST_LARGE_FILE) == fmd5('downloaded_' + TEST_LARGE_FILE + '_0')

        # download large file from multiple sources
        is_success, _ = await peers[2].download(TEST_LARGE_FILE, 'downloaded_' + TEST_LARGE_FILE + '_2')
        assert os.path.exists('downloaded_' + TEST_LARGE_FILE + '_2')
        assert is_success
        assert fmd5(TEST_LARGE_FILE) == fmd5('downloaded_' + TEST_LARGE_FILE + '_2')

        # download large file concurrently
        download_task_1 = peers[3].download(TEST_LARGE_FILE, 'downloaded_' + TEST_LARGE_FILE + '_3')
        download_task_2 = peers[4].download(TEST_LARGE_FILE, 'downloaded_' + TEST_LARGE_FILE + '_4')
        (is_success_1, _), (is_success_2, _) = await asyncio.gather(download_task_1, download_task_2)
        assert os.path.exists('downloaded_' + TEST_LARGE_FILE + '_3')
        assert is_success_1
        assert fmd5(TEST_LARGE_FILE) == fmd5('downloaded_' + TEST_LARGE_FILE + '_3')
        assert os.path.exists('downloaded_' + TEST_LARGE_FILE + '_4')
        assert is_success_2
        assert fmd5(TEST_LARGE_FILE) == fmd5('downloaded_' + TEST_LARGE_FILE + '_4')
    finally:
        try:
            os.remove('downloaded_' + TEST_SMALL_FILE)
        except FileNotFoundError:
            pass
        try:
            os.remove('downloaded_' + TEST_LARGE_FILE + '_0')
        except FileNotFoundError:
            pass
        try:
            os.remove('downloaded_' + TEST_LARGE_FILE + '_2')
        except FileNotFoundError:
            pass
        try:
            os.remove('downloaded_' + TEST_LARGE_FILE + '_3')
        except FileNotFoundError:
            pass
        try:
            os.remove('downloaded_' + TEST_LARGE_FILE + '_4')
        except FileNotFoundError:
            pass

        await tracker.stop()
        await asyncio.gather(*[peer.stop() for peer in peers])


async def test_delay(unused_tcp_port):
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
    is_success, _ = await peers[0].publish(TEST_SMALL_FILE_1)
    assert is_success
    file_list = tracker.file_list()
    assert TEST_SMALL_FILE in file_list
    assert file_list[TEST_SMALL_FILE_1]['size'] == TEST_SMALL_FILE_SIZE
    file_list = await peers[1].list_file()
    assert TEST_SMALL_FILE_1 in file_list

    # download small file
    start = time.time()
    result, msg = await peers[1].download(TEST_SMALL_FILE, 'downloaded_' + TEST_SMALL_FILE)
    assert result is True
    assert os.path.exists('downloaded_' + TEST_SMALL_FILE)
    assert fmd5(TEST_SMALL_FILE) == fmd5('downloaded_' + TEST_SMALL_FILE)
    os.remove('downloaded_' + TEST_SMALL_FILE)
    download_time = time.time() - start
    start = time.time()
    peers[0].set_delay(1)
    result, msg = await peers[1].download(TEST_SMALL_FILE_1, 'downloaded_' + TEST_SMALL_FILE_1)
    assert result is True
    download_time_with_delay = time.time() - start
    assert download_time_with_delay > download_time
    peers[0].set_delay(0)

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
    await asyncio.sleep(1)
    assert TEST_SMALL_FILE not in tracker.file_list()

    await tracker.stop()
