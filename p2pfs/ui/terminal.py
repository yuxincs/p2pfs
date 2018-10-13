from asyncio import IncompleteReadError
from beautifultable import BeautifulTable
from p2pfs.core.tracker import Tracker
from p2pfs.core.peer import Peer
from p2pfs.core.exceptions import *
import p2pfs.ui.aiocmd as aiocmd
import logging


class TrackerTerminal(aiocmd.Cmd):
    INTRO = 'Welcome to \033[1mTracker\033[0m terminal.    Type help or ? to list commands.\n'
    PROMPT = '\033[1mTracker>\033[0m '

    def __init__(self, tracker):
        assert isinstance(tracker, Tracker)
        self._tracker = tracker
        super().__init__()

    async def do_start(self, arg):
        arg = arg.split(' ')
        if len(arg) < 2:
            print('Not enough argument, start <host> <port>')
        else:
            await self._tracker.start((arg[0], int(arg[1])))

    async def do_list_files(self, arg):
        file_list_dict = self._tracker.file_list()
        table = BeautifulTable()
        table.row_separator_char = ''

        for filename, fileinfo in file_list_dict.items():
            if table.column_count == 0:
                table.column_headers = ['Filename'] + list(map(lambda x: x.capitalize(), tuple(fileinfo.keys())))
            table.append_row((filename, ) + tuple(fileinfo.values()))
        print(table)

    async def do_list_peers(self, arg):
        table = BeautifulTable()
        table.row_separator_char = ''
        table.column_headers = ['Peer Address']
        for peer in self._tracker.peers():
            table.append_row([peer])
        print(table)

    async def do_list_chunkinfo(self, arg):
        # TODO: pretty print chunk info
        print(self._tracker.chunkinfo())

    async def do_exit(self, arg):
        await self._tracker.stop()
        return True


class PeerTerminal(aiocmd.Cmd):
    INTRO = 'Welcome to \033[1mPeer\033[0m terminal.    Type help or ? to list commands.\n'
    PROMPT = '\033[1mPeer>\033[0m '

    def __init__(self, peer):
        assert isinstance(peer, Peer)
        self._peer = peer
        super().__init__()

    async def do_publish(self, arg):
        arg = arg.split(' ')[0]
        try:
            await self._peer.publish(arg)
        except FileNotFoundError:
            print('File {} doesn\'t exist.'.format(arg))
        except FileExistsError:
            print('File {} already registered on tracker, use \'list_files\' to see.'.format(arg))
        except TrackerNotConnectedError:
            print('Tracker is not connected. Use \'connect <tracker_ip> <tracker_port> to connect.\' ')
        except (ConnectionError, RuntimeError, IncompleteReadError):
            print('Error occurred during communications with tracker, try to re-connect.')\
        except InProgressError:
            print('Publish file {} already in progress.'.format(arg))
        else:
            print('File {} successfully published on tracker.'.format(arg))

    async def do_set_delay(self, arg):
        arg = arg.split(' ')[0]
        if arg == '':
            print('delay is required.')
        else:
            self._peer.set_delay(float(arg))

    async def do_connect(self, arg):
        arg = arg.split(' ')
        if len(arg) < 2:
            print('More arguments required! Usage: connect <address> <port>')
        _, message = await self._peer.connect((arg[0], int(arg[1])))
        print(message)

    async def do_list_files(self, arg):
        file_list_dict, _ = await self._peer.list_file()
        table = BeautifulTable()
        table.row_separator_char = ''

        for filename, fileinfo in file_list_dict.items():
            if table.column_count == 0:
                table.column_headers = ['Filename'] + list(map(lambda x: x.capitalize(), tuple(fileinfo.keys())))
            table.append_row((filename,) + tuple(fileinfo.values()))
        print(table)

    async def do_download(self, arg):
        filename, destination, *_ = arg.split(' ')
        from tqdm import tqdm

        def tqdm_hook_wrapper(t):
            last_chunk = [0]

            def update_to(chunknum=1, chunksize=1, tsize=None):
                if tsize is not None:
                    t.total = tsize
                t.update((chunknum - last_chunk[0]) * chunksize)
                last_chunk[0] = chunknum

            return update_to

        with tqdm(unit='B', unit_scale=True, unit_divisor=1024, miniters=1, desc='Downloading ...') as t:
            # no report hook if we need debug logging (too many logs will cause trouble to tqdm)
            hook = tqdm_hook_wrapper(t) if logging.getLogger().getEffectiveLevel() != logging.DEBUG else None

            _, message = await self._peer.download(filename, destination, reporthook=hook)

        print(message)

    async def do_exit(self, arg):
        await self._peer.stop()
        return True
