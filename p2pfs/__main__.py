import asyncio
import argparse
import logging
import coloredlogs
import uvloop
from p2pfs.core.peer import Peer
from p2pfs.core.tracker import Tracker
from p2pfs.ui.terminal import TrackerTerminal, PeerTerminal

coloredlogs.install(level='ERROR', fmt='%(levelname)s:%(module)s: %(message)s')
asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


def main():
    arg_parser = argparse.ArgumentParser(description=__doc__)
    arg_parser.add_argument('option', metavar='OPTION', type=str, nargs=1)
    arg_parser.add_argument('host', metavar='HOST', type=str, nargs='?', default='127.0.0.1')
    arg_parser.add_argument('host_port', metavar='HOST_PORT', type=int, nargs='?', default=8888)
    arg_parser.add_argument('server', metavar='SERVER', type=str, nargs='?', default='127.0.0.1')
    arg_parser.add_argument('server_port', metavar='SERVER_PORT', type=int, nargs='?', default=8888)
    results = arg_parser.parse_args()

    loop = asyncio.get_event_loop()
    terminal = None
    if results.option[0] == 'server':
        tracker = Tracker(results.host, results.host_port)
        loop.run_until_complete(tracker.start())
        terminal = TrackerTerminal(tracker)
    elif results.option[0] == 'peer':
        peer = Peer(results.host, results.host_port, results.server, results.server_port)
        loop.run_until_complete(peer.start())
        terminal = PeerTerminal(peer)
    else:
        logging.error('Option must either be \'server\' or \'peer\'')
    try:
        loop.run_until_complete(terminal.cmdloop())
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()


if __name__ == '__main__':
    main()
