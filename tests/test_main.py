import os
import hashlib
from p2pfs import Peer, Tracker


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


def test_main():
    peer_1, peer_2 = Peer('localhost', 0, 'localhost', 8880), Peer('localhost', 0, 'localhost', 8880)
    tracker = Tracker('localhost', 8880)
    with open('test_small_file', 'wb') as fout:
        fout.write(os.urandom(1000))

    with open('test_big_file', 'wb') as fout:
        # write 500MB random data into the file
        for _ in range(500):
            fout.write(os.urandom(1000 * 1000))

    tracker.start()
    peer_1.start()
    peer_2.start()

    # peer1 publish small file and peer2 downloads it
    peer_1.publish('test_small_file')
    file_list = tracker.file_list()
    assert 'test_small_file' in file_list
    assert file_list['test_small_file']['size'] == 1000
    file_list = peer_2.list_file()
    assert 'test_small_file' in file_list
    result, msg = peer_2.download('test_small_file', 'downloaded_small_file')
    assert result is True
    assert os.path.exists('downloaded_small_file')
    assert fmd5('test_small_file') == fmd5('downloaded_small_file')

    peer_2.publish('test_big_file')
    file_list = tracker.file_list()
    assert 'test_big_file' in file_list and 'test_small_file' in file_list
    assert file_list['test_big_file']['size'] == 500 * 1000 * 1000
    file_list = peer_1.list_file()
    assert 'test_big_file' in file_list and 'test_small_file' in file_list
    result, msg = peer_1.download('test_big_file', 'downloaded_big_file')
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
