from p2pfs.core.peer import Peer
from p2pfs.core.tracker import Tracker
from p2pfs.ui.terminal import TrackerTerminal, PeerTerminal
import argparse
import logging
import coloredlogs

coloredlogs.install(level='DEBUG', fmt='%(levelname)s:%(module)s[0x%(thread)x]: %(message)s')


def main():
    arg_parser = argparse.ArgumentParser(description=__doc__)
    arg_parser.add_argument('option', metavar='OPTION', type=str, nargs=1)
    arg_parser.add_argument('host', metavar='HOST', type=str, nargs='?', default='127.0.0.1')
    arg_parser.add_argument('host_port', metavar='HOST_PORT', type=int, nargs='?', default=8888)
    arg_parser.add_argument('server', metavar='SERVER', type=str, nargs='?', default='127.0.0.1')
    arg_parser.add_argument('server_port', metavar='SERVER_PORT', type=int, nargs='?', default=8888)
    results = arg_parser.parse_args()

    if results.option[0] == 'server':
        tracker = Tracker(results.host, results.host_port)
        tracker.start()
        terminal = TrackerTerminal(tracker)
        terminal.cmdloop()
    elif results.option[0] == 'peer':
        peer = Peer(results.host, results.host_port, results.server, results.server_port)
        peer.start()
        terminal = PeerTerminal(peer)
        terminal.cmdloop()
    else:
        logging.error('Option must either be \'server\' or \'peer\'')


if __name__ == '__main__':
    main()
