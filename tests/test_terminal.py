import asyncio
import os
import uvloop
from tests.conftest import TEST_SMALL_FILE
from p2pfs import Peer, Tracker, PeerTerminal, TrackerTerminal

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

loop = asyncio.get_event_loop()
tracker = Tracker('localhost', 8880)
tracker_terminal = TrackerTerminal(tracker)
peers = tuple(Peer('localhost', 0, 'localhost', 8880) for _ in range(3))
peer_terminals = tuple(PeerTerminal(peer) for peer in peers)


def steup_module(module):
    tracker_started = loop.run_until_complete(tracker.start())
    # spawn peers concurrently
    peers_started = \
        loop.run_until_complete(asyncio.gather(*[peer.start() for peer in peers]))
    assert tracker_started and all(peers_started)


def teardown_module(module):
    """ stop the peers and tracker"""
    loop.run_until_complete(asyncio.wait({peer.stop() for peer in peers}))
    loop.run_until_complete(tracker.stop())
    loop.close()


def test_terminals():
    loop.run_until_complete(peer_terminals[0].do_help(''))
    loop.run_until_complete(peer_terminals[0].do_publish(TEST_SMALL_FILE))
    loop.run_until_complete(peer_terminals[1].do_download(TEST_SMALL_FILE + ' ' + 'downloaded_' + TEST_SMALL_FILE))
    assert os.path.exists('downloaded_' + TEST_SMALL_FILE)
    os.remove('downloaded_' + TEST_SMALL_FILE)
