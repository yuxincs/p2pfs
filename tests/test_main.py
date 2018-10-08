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


def test_main():
    tracker_started = loop.run_until_complete(tracker.start())
    # spawn peers concurrently
    peers_started = \
        loop.run_until_complete(asyncio.gather(*[peer.start() for peer in peers]))
    assert tracker_started and all(peers_started)

    # peer1 publish small file and peer2 downloads it
    loop.run_until_complete(peers[0].publish(TEST_SMALL_FILE))
    file_list = tracker.file_list()
    assert TEST_SMALL_FILE in file_list
    assert file_list[TEST_SMALL_FILE]['size'] == 1000
    file_list = loop.run_until_complete(peers[1].list_file())
    assert TEST_SMALL_FILE in file_list

    def reporthook(chunk_num, chunk_size, total_size):
        reporthook.value = (chunk_num, total_size)

    # download small file
    result, msg = loop.run_until_complete(
        peers[1].download('test_small_file', 'downloaded_small_file', reporthook=reporthook)
    )
    assert result is True
    assert os.path.exists('downloaded_small_file')
    assert fmd5('test_small_file') == fmd5('downloaded_small_file')
    assert reporthook.value == (1, 1000)

    # publish big file
    loop.run_until_complete(peers[1].publish('test_big_file'))
    file_list = tracker.file_list()
    assert 'test_big_file' in file_list and 'test_small_file' in file_list
    assert file_list['test_big_file']['size'] == 500 * 1000 * 1000
    file_list = loop.run_until_complete(peers[0].list_file())
    assert 'test_big_file' in file_list and 'test_small_file' in file_list

    # download from single peer
    result, msg = loop.run_until_complete(peers[0].download('test_big_file', 'downloaded_big_file'))
    assert result is True
    assert os.path.exists('downloaded_big_file')
    assert fmd5('test_big_file') == fmd5('downloaded_big_file')
    
    # test download from multiple peers
    result, msg = loop.run_until_complete(peers[2].download('test_big_file', 'downloaded_big_file_from_multiple_peers'))
    assert result is True
    assert os.path.exists('downloaded_big_file_from_multiple_peers')
    assert fmd5('test_big_file') == fmd5('downloaded_big_file_from_multiple_peers')
    os.remove('downloaded_small_file')
    os.remove('downloaded_big_file')
    os.remove('downloaded_big_file_from_multiple_peers')
