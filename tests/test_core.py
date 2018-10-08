import os
import hashlib
import asyncio
import uvloop
from p2pfs import Peer, Tracker
from tests.conftest import TEST_SMALL_FILE, TEST_LARGE_FILE

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

loop = asyncio.get_event_loop()
peers = tuple(Peer('localhost', 0, 'localhost', 8880) for _ in range(3))
tracker = Tracker('localhost', 8880)


def teardown_module(module):
    """ stop the peers and tracker"""
    loop.run_until_complete(asyncio.wait({peer.stop() for peer in peers}))
    loop.run_until_complete(tracker.stop())
    loop.close()


def fmd5(fname):
    """ calculate the md5 value of a file
    :param fname: file name
    :return: md5 value.
    """
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def test_server_refused():
    peer = Peer('localhost', 0, 'localhost', 8880)
    started = asyncio.get_event_loop().run_until_complete(peer.start())
    assert not started


def test_start():
    tracker_started = loop.run_until_complete(tracker.start())
    # spawn peers concurrently
    peers_started = \
        loop.run_until_complete(asyncio.gather(*[peer.start() for peer in peers]))
    assert tracker_started and all(peers_started)


def test_publish_small():
    # peer1 publish small file and peer2 downloads it
    loop.run_until_complete(peers[0].publish(TEST_SMALL_FILE))
    file_list = tracker.file_list()
    assert TEST_SMALL_FILE in file_list
    assert file_list[TEST_SMALL_FILE]['size'] == 1000
    file_list = loop.run_until_complete(peers[1].list_file())
    assert TEST_SMALL_FILE in file_list


def test_download_small():
    def reporthook(chunk_num, chunk_size, total_size):
        reporthook.value = (chunk_num, total_size)

    # download small file
    result, msg = loop.run_until_complete(
        peers[1].download(TEST_SMALL_FILE, 'downloaded_' + TEST_SMALL_FILE, reporthook=reporthook)
    )
    assert result is True
    assert os.path.exists('downloaded_' + TEST_SMALL_FILE)
    assert fmd5(TEST_SMALL_FILE) == fmd5('downloaded_' + TEST_SMALL_FILE)
    assert reporthook.value == (1, 1000)
    os.remove('downloaded_' + TEST_SMALL_FILE)


def test_publish_large():
    # publish big file
    loop.run_until_complete(peers[1].publish(TEST_LARGE_FILE))
    file_list = tracker.file_list()
    assert TEST_LARGE_FILE in file_list in file_list
    assert file_list[TEST_LARGE_FILE]['size'] == 500 * 1000 * 1000
    file_list = loop.run_until_complete(peers[0].list_file())
    assert TEST_LARGE_FILE in file_list


def test_download_large_single():
    # download from single peer
    result, msg = loop.run_until_complete(peers[0].download(TEST_LARGE_FILE, 'downloaded_' + TEST_LARGE_FILE))
    assert result is True
    assert os.path.exists('downloaded_' + TEST_LARGE_FILE)
    assert fmd5(TEST_LARGE_FILE) == fmd5('downloaded_' + TEST_LARGE_FILE)
    os.remove('downloaded_' + TEST_LARGE_FILE)


def test_download_large_multi():
    # test download from multiple peers
    result, msg = loop.run_until_complete(peers[2].download(TEST_LARGE_FILE,
                                                            'downloaded_'+ TEST_LARGE_FILE + '_from_multiple_peers'))
    assert result is True
    assert os.path.exists('downloaded_' + TEST_LARGE_FILE + '_from_multiple_peers')
    assert fmd5(TEST_LARGE_FILE) == fmd5('downloaded_' + TEST_LARGE_FILE + '_from_multiple_peers')
    os.remove('downloaded_' + TEST_LARGE_FILE + '_from_multiple_peers')
