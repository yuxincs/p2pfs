import pytest
import os
import asyncio
import hashlib
import uvloop

TEST_SMALL_FILE = 'test_small_file'
TEST_LARGE_FILE = 'test_large_file'
TEST_SMALL_FILE_SIZE = 1000
TEST_LARGE_FILE_SIZE = 500 * 1000 * 1000


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


@pytest.fixture(scope='session', autouse=True)
def create_test_files(request):
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    with open(TEST_SMALL_FILE, 'wb') as fout:
        fout.write(os.urandom(1000))

    with open(TEST_LARGE_FILE, 'wb') as fout:
        # write 500MB random data into the file
        for _ in range(500):
            fout.write(os.urandom(1000 * 1000))

    def delete_files():
        os.remove(TEST_SMALL_FILE)
        os.remove(TEST_LARGE_FILE)

    request.addfinalizer(delete_files)
