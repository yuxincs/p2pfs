import asyncio
import os
import pytest
from tests.conftest import fmd5, TEST_SMALL_FILE
from p2pfs import Peer, Tracker, PeerTerminal, TrackerTerminal

pytestmark = pytest.mark.asyncio


async def test_terminals(unused_tcp_port, capsys):
    tracker = Tracker('localhost', unused_tcp_port)
    peers = tuple(Peer('localhost', 0, 'localhost', unused_tcp_port) for _ in range(2))
    tracker_started = await tracker.start()
    # spawn peers concurrently
    peers_started = await asyncio.gather(*[peer.start() for peer in peers])
    assert tracker_started and all(peers_started)
    peer_terminals = tuple(PeerTerminal(peer) for peer in peers)
    tracker_terminal = TrackerTerminal(tracker)

    await peer_terminals[0].do_help('')
    capsys.readouterr()
    await peer_terminals[0].do_publish(TEST_SMALL_FILE)
    out, _ = capsys.readouterr()
    assert out == 'Success\n'
    # TODO: fix resource warning for do_list_files(), possibly due to BeautifulTable()
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
    os.remove('downloaded_' + TEST_SMALL_FILE)

    await tracker_terminal.do_exit('')
    await asyncio.gather(*[terminal.do_exit('') for terminal in peer_terminals])
