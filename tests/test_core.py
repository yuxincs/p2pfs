import os
import asyncio
import pytest
from p2pfs import *
import time
from tests.conftest import fmd5, setup_tracker_and_peers, TEST_SMALL_FILE, TEST_LARGE_FILE, \
    TEST_SMALL_FILE_SIZE, TEST_LARGE_FILE_SIZE, TEST_SMALL_FILE_1
pytestmark = pytest.mark.asyncio


def cleanup_files(files):
    for file in files:
        try:
            os.remove(file)
        except FileNotFoundError:
            pass


async def test_connect_refused(unused_tcp_port):
    peer = Peer()
    await peer.start(('localhost', 0))
    with pytest.raises(ConnectionRefusedError):
        await peer.connect(('localhost', unused_tcp_port))


async def test_start_stop(unused_tcp_port):
    tracker, peers = await setup_tracker_and_peers(1, unused_tcp_port)

    await tracker.stop()
    await asyncio.gather(*[peer.stop() for peer in peers])


async def test_publish_refuse(unused_tcp_port):
    tracker, peers = await setup_tracker_and_peers(1, unused_tcp_port)
    try:
        with open('test_publish_refuse', 'wb') as fout:
            fout.write(os.urandom(100))
        await peers[0].publish('test_publish_refuse')
        with pytest.raises(FileNotFoundError):
            await peers[0].publish('__not_existed_file')
        with pytest.raises(FileExistsError):
            await peers[0].publish('test_publish_refuse')
        os.remove('test_publish_refuse')
    finally:
        await tracker.stop()
        await asyncio.gather(*[peer.stop() for peer in peers])


async def test_publish(unused_tcp_port):
    tracker, peers = await setup_tracker_and_peers(2, unused_tcp_port)
    try:
        # peer0 publishes a small_file and peer1 publishes a large file
        await peers[0].publish(TEST_SMALL_FILE)
        file_list = tracker.file_list()
        assert TEST_SMALL_FILE in file_list
        assert file_list[TEST_SMALL_FILE]['size'] == TEST_SMALL_FILE_SIZE
        file_list= await peers[1].list_file()
        assert TEST_SMALL_FILE in file_list

        await peers[1].publish(TEST_LARGE_FILE)
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
        await peers[0].publish(TEST_SMALL_FILE)
        file_list = tracker.file_list()
        assert TEST_SMALL_FILE in file_list
        assert file_list[TEST_SMALL_FILE]['size'] == TEST_SMALL_FILE_SIZE
        file_list = await peers[1].list_file()
        assert TEST_SMALL_FILE in file_list

        await peers[1].publish(TEST_LARGE_FILE)
        file_list = tracker.file_list()
        assert TEST_LARGE_FILE in file_list and TEST_SMALL_FILE in file_list
        assert file_list[TEST_LARGE_FILE]['size'] == TEST_LARGE_FILE_SIZE
        file_list = await peers[0].list_file()
        assert TEST_LARGE_FILE in file_list and TEST_SMALL_FILE in file_list

        def reporthook(chunk_num, chunk_size, total_size):
            reporthook.value = (chunk_num, total_size)

        # download small file
        to_cleanup.add('downloaded_' + TEST_SMALL_FILE)
        await peers[1].download(TEST_SMALL_FILE, 'downloaded_' + TEST_SMALL_FILE, reporthook=reporthook)
        assert os.path.exists('downloaded_' + TEST_SMALL_FILE)
        assert fmd5(TEST_SMALL_FILE) == fmd5('downloaded_' + TEST_SMALL_FILE)
        assert reporthook.value == (1, 1000)

        # download large file from single source
        to_cleanup.add('downloaded_' + TEST_LARGE_FILE + '_0')
        await peers[0].download(TEST_LARGE_FILE, 'downloaded_' + TEST_LARGE_FILE + '_0')
        assert os.path.exists('downloaded_' + TEST_LARGE_FILE + '_0')
        assert fmd5(TEST_LARGE_FILE) == fmd5('downloaded_' + TEST_LARGE_FILE + '_0')

        # download large file from multiple sources
        to_cleanup.add('downloaded_' + TEST_LARGE_FILE + '_2')
        await peers[2].download(TEST_LARGE_FILE, 'downloaded_' + TEST_LARGE_FILE + '_2')
        assert os.path.exists('downloaded_' + TEST_LARGE_FILE + '_2')
        assert fmd5(TEST_LARGE_FILE) == fmd5('downloaded_' + TEST_LARGE_FILE + '_2')

        # download large file concurrently
        to_cleanup.add('downloaded_' + TEST_LARGE_FILE + '_3')
        to_cleanup.add('downloaded_' + TEST_LARGE_FILE + '_4')
        download_task_1 = peers[3].download(TEST_LARGE_FILE, 'downloaded_' + TEST_LARGE_FILE + '_3')
        download_task_2 = peers[4].download(TEST_LARGE_FILE, 'downloaded_' + TEST_LARGE_FILE + '_4')
        await asyncio.gather(download_task_1, download_task_2)
        assert os.path.exists('downloaded_' + TEST_LARGE_FILE + '_3')
        assert fmd5(TEST_LARGE_FILE) == fmd5('downloaded_' + TEST_LARGE_FILE + '_3')
        assert os.path.exists('downloaded_' + TEST_LARGE_FILE + '_4')
        assert fmd5(TEST_LARGE_FILE) == fmd5('downloaded_' + TEST_LARGE_FILE + '_4')
    finally:
        cleanup_files(to_cleanup)
        await tracker.stop()
        await asyncio.gather(*[peer.stop() for peer in peers])


async def test_delay(unused_tcp_port):
    tracker, peers = await setup_tracker_and_peers(2, unused_tcp_port)

    # peer0 publishes a small_file and peer1 publishes a large file
    await peers[0].publish(TEST_SMALL_FILE)
    file_list = tracker.file_list()
    assert TEST_SMALL_FILE in file_list
    assert file_list[TEST_SMALL_FILE]['size'] == TEST_SMALL_FILE_SIZE
    file_list = await peers[1].list_file()
    assert TEST_SMALL_FILE in file_list
    await peers[0].publish(TEST_SMALL_FILE_1)
    file_list = tracker.file_list()
    assert TEST_SMALL_FILE in file_list
    assert file_list[TEST_SMALL_FILE_1]['size'] == TEST_SMALL_FILE_SIZE
    file_list = await peers[1].list_file()
    assert TEST_SMALL_FILE_1 in file_list
    to_cleanup = set()
    try:
        # download small file
        start = time.time()
        to_cleanup.add('downloaded_' + TEST_SMALL_FILE)
        await peers[1].download(TEST_SMALL_FILE, 'downloaded_' + TEST_SMALL_FILE)
        assert os.path.exists('downloaded_' + TEST_SMALL_FILE)
        assert fmd5(TEST_SMALL_FILE) == fmd5('downloaded_' + TEST_SMALL_FILE)

        download_time = time.time() - start
        start = time.time()
        peers[0].set_delay(1)
        to_cleanup.add('downloaded_' + TEST_SMALL_FILE_1)
        await peers[1].download(TEST_SMALL_FILE_1, 'downloaded_' + TEST_SMALL_FILE_1)
        download_time_with_delay = time.time() - start
        assert download_time_with_delay > download_time
        peers[0].set_delay(0)
    finally:
        cleanup_files(to_cleanup)
        await tracker.stop()
        await asyncio.gather(*[peer.stop() for peer in peers])


async def test_peer_disconnect(unused_tcp_port):
    tracker, peers = await setup_tracker_and_peers(1, unused_tcp_port)
    try:
        await peers[0].publish(TEST_SMALL_FILE)
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
        await peers[0].publish(TEST_LARGE_FILE)
        assert TEST_LARGE_FILE in tracker.file_list()

        # download large file from single source
        to_cleanup.add('downloaded_' + TEST_LARGE_FILE + '_1')
        await peers[1].download(TEST_LARGE_FILE, 'downloaded_' + TEST_LARGE_FILE + '_1')
        assert os.path.exists('downloaded_' + TEST_LARGE_FILE + '_1')
        assert fmd5(TEST_LARGE_FILE) == fmd5('downloaded_' + TEST_LARGE_FILE + '_1')

        peers[1].set_delay(0.1)

        # stop the peer_0, peer_2 should continue to download because peer_1 has all chunks of the file
        # but the speed will be noticeably slower because peer_1 has delay
        async def stop_peer_after(peer, delay):
            await asyncio.sleep(delay)
            await peer.stop()
        # run download and stop peer task concurrently
        to_cleanup.add('downloaded_' + TEST_LARGE_FILE + '_2')
        await asyncio.gather(peers[2].download(TEST_LARGE_FILE, 'downloaded_' + TEST_LARGE_FILE + '_2'),
                                                  stop_peer_after(peers[0], 1))
        assert os.path.exists('downloaded_' + TEST_LARGE_FILE + '_2')
        assert fmd5(TEST_LARGE_FILE) == fmd5('downloaded_' + TEST_LARGE_FILE + '_2')
    finally:
        cleanup_files(to_cleanup)
        await tracker.stop()
        await asyncio.gather(*[peer.stop() for peer in peers[1:]])


async def test_tracker_download_disconnect(unused_tcp_port):
    tracker, peers = await setup_tracker_and_peers(2, unused_tcp_port)
    to_cleanup = set()
    try:
        await peers[0].publish(TEST_LARGE_FILE)
        assert TEST_LARGE_FILE in tracker.file_list()

        # stop tracker in the download process, should still successfully download since peer is still alive
        async def stop_after(obj, delay):
            await asyncio.sleep(delay)
            await obj.stop()
        # run download and stop task concurrently
        to_cleanup.add('downloaded_' + TEST_LARGE_FILE + '_2')
        await asyncio.gather(peers[1].download(TEST_LARGE_FILE, 'downloaded_' + TEST_LARGE_FILE + '_2'),
                                                  stop_after(tracker, 1))
        assert os.path.exists('downloaded_' + TEST_LARGE_FILE + '_2')
        assert fmd5(TEST_LARGE_FILE) == fmd5('downloaded_' + TEST_LARGE_FILE + '_2')
    finally:
        cleanup_files(to_cleanup)
        await asyncio.gather(*[peer.stop() for peer in peers])


async def test_peer_restart(unused_tcp_port):
    tracker, peers = await setup_tracker_and_peers(1, unused_tcp_port)
    await peers[0].publish(TEST_SMALL_FILE)
    assert TEST_SMALL_FILE in tracker.file_list()
    await peers[0].disconnect()
    await asyncio.sleep(0.5)
    assert TEST_SMALL_FILE not in tracker.file_list()
    await peers[0].connect(('localhost', unused_tcp_port))
    await peers[0].publish(TEST_SMALL_FILE)
    assert TEST_SMALL_FILE in tracker.file_list()


async def test_tracker_restart(unused_tcp_port):
    tracker, peers = await setup_tracker_and_peers(2, unused_tcp_port)
    await tracker.stop()
    assert not tracker.is_running()
    await asyncio.sleep(0.5)
    is_all_connected = await asyncio.gather(*[peer.is_connected() for peer in peers])
    assert not any(is_all_connected)
    with pytest.raises(TrackerNotConnectedError):
        await peers[0].publish(TEST_SMALL_FILE)
    with pytest.raises(TrackerNotConnectedError):
        await peers[1].list_file()
    await tracker.start(('localhost', unused_tcp_port))
    assert tracker.is_running()
