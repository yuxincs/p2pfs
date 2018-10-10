import os
import asyncio
import pytest
from p2pfs import Peer, Tracker
import time
from tests.conftest import fmd5, TEST_SMALL_FILE, TEST_LARGE_FILE, \
    TEST_SMALL_FILE_SIZE, TEST_LARGE_FILE_SIZE, TEST_SMALL_FILE_1
pytestmark = pytest.mark.asyncio


async def setup_tracker_and_peers(peer_num, tracker_port):
    tracker = Tracker()
    peers = tuple(Peer() for _ in range(peer_num))
    tracker_started = await tracker.start('localhost', tracker_port)
    # spawn peers concurrently
    peers_started = await asyncio.gather(*[peer.start('localhost', 0) for peer in peers])
    assert tracker_started and all(peers_started)
    peers_connected = await asyncio.gather(*[peer.connect(('localhost', tracker_port)) for peer in peers])
    assert all(peers_connected)
    return tracker, peers


def cleanup_files(files):
    for file in files:
        try:
            os.remove(file)
        except FileNotFoundError:
            pass


async def test_server_refused(unused_tcp_port):
    peer = Peer()
    started = await peer.start('localhost', 0)
    assert started
    is_success, _ = peer.connect('localhost', unused_tcp_port)
    assert not is_success


async def test_start_stop(unused_tcp_port):
    tracker, peers = await setup_tracker_and_peers(1, unused_tcp_port)

    await tracker.stop()
    await asyncio.gather(*[peer.stop() for peer in peers])


async def test_publish_refuse(unused_tcp_port):
    tracker, peers = await setup_tracker_and_peers(1, unused_tcp_port)
    try:
        with open('test_publish_refuse', 'wb') as fout:
            fout.write(os.urandom(100))
        is_success, _ = await peers[0].publish('test_publish_refuse')
        assert is_success
        is_success, _ = await peers[0].publish('test_publish_refuse')
        assert not is_success
        os.remove('test_publish_refuse')
    finally:
        await tracker.stop()
        await asyncio.gather(*[peer.stop() for peer in peers])


async def test_publish(unused_tcp_port):
    tracker, peers = await setup_tracker_and_peers(2, unused_tcp_port)
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
    finally:
        await tracker.stop()
        await asyncio.gather(*[peer.stop() for peer in peers])


async def test_download(unused_tcp_port):
    tracker, peers = await setup_tracker_and_peers(5, unused_tcp_port)
    to_cleanup = set()
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
        to_cleanup.add('downloaded_' + TEST_SMALL_FILE)

        # download large file from single source
        is_success, _ = await peers[0].download(TEST_LARGE_FILE, 'downloaded_' + TEST_LARGE_FILE + '_0')
        assert os.path.exists('downloaded_' + TEST_LARGE_FILE + '_0')
        assert is_success
        assert fmd5(TEST_LARGE_FILE) == fmd5('downloaded_' + TEST_LARGE_FILE + '_0')
        to_cleanup.add('downloaded_' + TEST_LARGE_FILE + '_0')

        # download large file from multiple sources
        is_success, _ = await peers[2].download(TEST_LARGE_FILE, 'downloaded_' + TEST_LARGE_FILE + '_2')
        assert os.path.exists('downloaded_' + TEST_LARGE_FILE + '_2')
        assert is_success
        assert fmd5(TEST_LARGE_FILE) == fmd5('downloaded_' + TEST_LARGE_FILE + '_2')
        to_cleanup.add('downloaded_' + TEST_LARGE_FILE + '_2')

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
        to_cleanup.add('downloaded_' + TEST_LARGE_FILE + '_3')
        to_cleanup.add('downloaded_' + TEST_LARGE_FILE + '_4')
    finally:
        cleanup_files(to_cleanup)
        await tracker.stop()
        await asyncio.gather(*[peer.stop() for peer in peers])


async def test_delay(unused_tcp_port):
    tracker, peers = await setup_tracker_and_peers(2, unused_tcp_port)

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
    to_cleanup = set()
    try:
        # download small file
        start = time.time()
        result, msg = await peers[1].download(TEST_SMALL_FILE, 'downloaded_' + TEST_SMALL_FILE)
        assert result is True
        assert os.path.exists('downloaded_' + TEST_SMALL_FILE)
        assert fmd5(TEST_SMALL_FILE) == fmd5('downloaded_' + TEST_SMALL_FILE)
        to_cleanup.add('downloaded_' + TEST_SMALL_FILE)

        download_time = time.time() - start
        start = time.time()
        peers[0].set_delay(1)
        result, msg = await peers[1].download(TEST_SMALL_FILE_1, 'downloaded_' + TEST_SMALL_FILE_1)
        assert result is True
        download_time_with_delay = time.time() - start
        assert download_time_with_delay > download_time
        peers[0].set_delay(0)
        to_cleanup.add('downloaded_' + TEST_SMALL_FILE_1)
    finally:
        cleanup_files(to_cleanup)
        await tracker.stop()
        await asyncio.gather(*[peer.stop() for peer in peers])


async def test_peer_disconnect(unused_tcp_port):
    tracker, peers = await setup_tracker_and_peers(1, unused_tcp_port)
    try:
        is_suceess, _ = await peers[0].publish(TEST_SMALL_FILE)
        assert is_suceess
        assert TEST_SMALL_FILE in tracker.file_list()

        # stop peer and check the file has been removed
        await peers[0].stop()
        # return control to the loop for tracker code to run
        await asyncio.sleep(1)
        assert TEST_SMALL_FILE not in tracker.file_list()
    finally:
        await tracker.stop()


async def test_peer_download_disconnect(unused_tcp_port):
    tracker, peers = await setup_tracker_and_peers(3, unused_tcp_port)
    to_cleanup = set()
    try:
        is_suceess, _ = await peers[0].publish(TEST_LARGE_FILE)
        assert is_suceess
        assert TEST_LARGE_FILE in tracker.file_list()

        # download large file from single source
        is_success, _ = await peers[1].download(TEST_LARGE_FILE, 'downloaded_' + TEST_LARGE_FILE + '_1')
        assert os.path.exists('downloaded_' + TEST_LARGE_FILE + '_1')
        assert is_success
        assert fmd5(TEST_LARGE_FILE) == fmd5('downloaded_' + TEST_LARGE_FILE + '_1')
        to_cleanup.add('downloaded_' + TEST_LARGE_FILE + '_1')

        peers[1].set_delay(0.1)

        # stop the peer_0, peer_2 should continue to download because peer_1 has all chunks of the file
        # but the speed will be noticeably slower because peer_1 has delay
        async def stop_peer_after(peer, delay):
            await asyncio.sleep(delay)
            await peer.stop()
        # run download and stop peer task concurrently
        (is_success, _), _ = await asyncio.gather(peers[2].download(TEST_LARGE_FILE, 'downloaded_' + TEST_LARGE_FILE + '_2'),
                                                  stop_peer_after(peers[0], 1))
        assert os.path.exists('downloaded_' + TEST_LARGE_FILE + '_2')
        assert is_success
        assert fmd5(TEST_LARGE_FILE) == fmd5('downloaded_' + TEST_LARGE_FILE + '_2')
        to_cleanup.add('downloaded_' + TEST_LARGE_FILE + '_2')
    finally:
        cleanup_files(to_cleanup)
        await tracker.stop()
        await asyncio.gather(*[peer.stop() for peer in peers[1:]])
