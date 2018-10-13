import asyncio
import os
import pytest
from tests.conftest import fmd5, setup_tracker_and_peers, TEST_SMALL_FILE
from p2pfs import PeerTerminal, TrackerTerminal

pytestmark = pytest.mark.asyncio


async def test_terminals(unused_tcp_port, capsys):
    tracker, peers = await setup_tracker_and_peers(2, unused_tcp_port)
    peer_terminals = tuple(PeerTerminal(peer) for peer in peers)
    tracker_terminal = TrackerTerminal(tracker)
    try:
        await peer_terminals[1].do_connect('localhost {}'.format(unused_tcp_port))
        out, _ = capsys.readouterr()
        assert 'already' in out
        await peer_terminals[0].do_help('')
        capsys.readouterr()
        await peer_terminals[0].do_publish(TEST_SMALL_FILE)
        out, _ = capsys.readouterr()
        assert 'success' in out
        await peer_terminals[1].do_list_files('')
        out, _ = capsys.readouterr()
        assert TEST_SMALL_FILE in out
        await tracker_terminal.do_list_files('')
        out, _ = capsys.readouterr()
        assert TEST_SMALL_FILE in out
        await peer_terminals[1].do_set_delay('0')
        await peer_terminals[1].do_download(TEST_SMALL_FILE + ' ' + 'downloaded_' + TEST_SMALL_FILE)
        assert os.path.exists('downloaded_' + TEST_SMALL_FILE)
        assert fmd5(TEST_SMALL_FILE) == fmd5('downloaded_' + TEST_SMALL_FILE)
        out, _ = capsys.readouterr()
    finally:
        try:
            os.remove('downloaded_' + TEST_SMALL_FILE)
        except FileNotFoundError:
            pass
        await tracker_terminal.do_exit('')
        await asyncio.gather(*[terminal.do_exit('') for terminal in peer_terminals])
