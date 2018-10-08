import pytest
import os

TEST_SMALL_FILE = 'test_small_file'
TEST_LARGE_FILE = 'test_large_file'
TEST_SMALL_FILE_SIZE = 1000
TEST_LARGE_FILE_SIZE = 500 * 1000 * 1000


@pytest.fixture(scope='session', autouse=True)
def create_test_files(request):
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
