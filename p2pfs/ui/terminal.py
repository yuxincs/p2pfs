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

    def do_list_chunkinfo(self, arg):
        print(self._tracker.chunkinfo())

    def do_exit(self, arg):
        self._tracker.stop()
        return True


class PeerTerminal(cmd.Cmd):
    intro = 'Welcome to \033[1mPeer\033[0m terminal.    Type help or ? to list commands.\n'
    prompt = '(peer) '

    def __init__(self, peer, completekey='tab', stdin=None, stdout=None):
        super().__init__(completekey, stdin, stdout)
        assert isinstance(peer, Peer)
        self._peer = peer

    def do_list_peers(self, arg):
        print(tabulate(list(enumerate(self._peer.peers())), headers=['Index', 'UUID']))

    def do_publish(self, arg):
        arg = arg.split(' ')[0]
        _, message = self._peer.publish(arg)
        print(message)

    def do_list_files(self, arg):
        file_list_dict = self._peer.list_file()
        file_list = []
        headers = ['Filename']
        for filename, fileinfo in file_list_dict.items():
            if len(headers) == 1:
                headers.extend(tuple(map(lambda x: x.capitalize(), tuple(fileinfo.keys()))))
            file_list.append((filename,) + tuple(fileinfo.values()))

        print(tabulate(file_list, headers=headers))

    def do_download(self, arg):
        filename, destionation, *_ = arg.split(' ')

        def progress(current, total):
            import time
            import sys

            if current == 1:
                progress.start = time.time()
                progress.cur_speed = 0
            else:
                progress.cur_speed = (Peer.CHUNK_SIZE) / (time.time() - progress.start)
                progress.start = time.time()

            speed_str = ''
            if progress.cur_speed < 1024:
                speed_str = '{0:>6.1f}  B/s'.format(progress.cur_speed)
            elif 1024 < progress.cur_speed < 1024 * 1024:
                speed_str = '{0:>6.1f} KB/s'.format(progress.cur_speed / 1024)
            elif 1024 * 1024 < progress.cur_speed < 1024 * 1024 * 1024:
                speed_str = '{0:>6.1f} MB/s'.format(progress.cur_speed / (1024 * 1024))

            percent = current / total
            progress_count = int(percent * 30)
            dot_count = 30 - progress_count - 1
            sys.stdout.write('Downloading {} '.format(filename))
            sys.stdout.write('[{}>{}]{: >3d}% {}\r'
                             .format(progress_count * '=', dot_count * '.', int(percent * 100), speed_str))
            sys.stdout.flush()
            if current == total:
                print('Downloading {} ['.format(filename) + 30 * '=' + '>] 100%')

        _, message = self._peer.download(filename, destionation, progress)
        print(message)

    def do_exit(self, arg):
        self._peer.stop()
        return True
