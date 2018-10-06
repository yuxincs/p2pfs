import os
import hashlib
import asyncio
from p2pfs import Peer, Tracker
import uvloop

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


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
    peer_1, peer_2 = Peer('localhost', 0, 'localhost', 8880), Peer('localhost', 0, 'localhost', 8880)
    tracker = Tracker('localhost', 8880)
    with open('test_small_file', 'wb') as fout:
        fout.write(os.urandom(1000))

    with open('test_big_file', 'wb') as fout:
        # write 500MB random data into the file
        for _ in range(500):
            fout.write(os.urandom(1000 * 1000))
    loop = asyncio.get_event_loop()
    tracker_started = loop.run_until_complete(tracker.start())
    # spawn 2 peers concurrently
    peer_1_started, peer_2_started = \
        loop.run_until_complete(asyncio.gather(peer_1.start(), peer_2.start(), loop=loop))
    assert tracker_started and peer_1_started and peer_2_started

    # peer1 publish small file and peer2 downloads it
    loop.run_until_complete(peer_1.publish('test_small_file'))
    file_list = tracker.file_list()
    assert 'test_small_file' in file_list
    assert file_list['test_small_file']['size'] == 1000
    file_list = loop.run_until_complete(peer_2.list_file())
    assert 'test_small_file' in file_list

    def reporthook(chunk_num, chunk_size, total_size):
        reporthook.value = (chunk_num, total_size)

    result, msg = loop.run_until_complete(
        peer_2.download('test_small_file', 'downloaded_small_file', reporthook=reporthook)
    )

    assert result is True
    assert os.path.exists('downloaded_small_file')
    assert fmd5('test_small_file') == fmd5('downloaded_small_file')
    assert reporthook.value == (1, 1000)

    loop.run_until_complete(peer_2.publish('test_big_file'))
    file_list = tracker.file_list()
    assert 'test_big_file' in file_list and 'test_small_file' in file_list
    assert file_list['test_big_file']['size'] == 500 * 1000 * 1000
    file_list = loop.run_until_complete(peer_1.list_file())
    assert 'test_big_file' in file_list and 'test_small_file' in file_list
    result, msg = loop.run_until_complete(peer_1.download('test_big_file', 'downloaded_big_file'))
    assert result is True
    assert os.path.exists('downloaded_big_file')
    assert fmd5('test_big_file') == fmd5('downloaded_big_file')

    os.remove('test_small_file')
    os.remove('test_big_file')
    os.remove('downloaded_small_file')
    os.remove('downloaded_big_file')
    peer_1.stop()
    peer_2.stop()
    tracker.stop()
    loop.close()
