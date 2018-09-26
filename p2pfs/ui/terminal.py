import cmd
from tabulate import tabulate
from p2pfs.core.tracker import Tracker
from p2pfs.core.peer import Peer


class TrackerTerminal(cmd.Cmd):
    intro = 'Welcome to \033[1mTracker\033[0m terminal.    Type help or ? to list commands.\n'
    prompt = '(tracker) '

    def __init__(self, tracker, completekey='tab', stdin=None, stdout=None):
        super().__init__(completekey, stdin, stdout)
        assert isinstance(tracker, Tracker)
        self._tracker = tracker

    def do_list_files(self, arg):
        file_list_dict = self._tracker.file_list()
        file_list = []
        headers = ['Filename']
        for filename, fileinfo in file_list_dict.items():
            if len(headers) == 1:
                headers.extend(tuple(map(lambda x: x.capitalize(), tuple(fileinfo.keys()))))
            file_list.append((filename, ) + tuple(fileinfo.values()))

        print(tabulate(file_list, headers=headers))

    def do_list_peers(self, arg):
        print(tabulate(self._tracker.peers(), headers=['UUID', 'IP/Port']))

    def do_list_chunkinfo(self, arg):
        print(self._tracker.chunkinfo())


class PeerTerminal(cmd.Cmd):
    intro = 'Welcome to \033[1mPeer\033[0m terminal.    Type help or ? to list commands.\n'
    prompt = '(peer) '

    def __init__(self, peer, completekey='tab', stdin=None, stdout=None):
        super().__init__(completekey, stdin, stdout)
        assert isinstance(peer, Peer)
        self._peer = peer

    def do_publish(self, arg):
        arg = arg.split(' ')[0]
        _, message = self._peer.publish(arg)
        print(message)

    def do_list_files(self, arg):
        print(self._peer.list_file())

    def do_download(self, arg):
        # TODO: implement download function
        pass
