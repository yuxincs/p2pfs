import asyncio
from tabulate import tabulate
from p2pfs.core.tracker import Tracker
from p2pfs.core.peer import Peer
import p2pfs.ui.aiocmd as aiocmd


class TrackerTerminal(aiocmd.Cmd):
    INTRO = 'Welcome to \033[1mTracker\033[0m terminal.    Type help or ? to list commands.\n'
    PROMPT = 'tracker > '

    def __init__(self, tracker):
        assert isinstance(tracker, Tracker)
        self._tracker = tracker
        super().__init__()

    async def do_list_files(self, arg):
        file_list_dict = self._tracker.file_list()
        file_list = []
        headers = ['Filename']
        for filename, fileinfo in file_list_dict.items():
            if len(headers) == 1:
                headers.extend(tuple(map(lambda x: x.capitalize(), tuple(fileinfo.keys()))))
            file_list.append((filename, ) + tuple(fileinfo.values()))

        print(tabulate(file_list, headers=headers))

    async def do_list_peers(self, arg):
        print(self._tracker.peers())

    async def do_list_chunkinfo(self, arg):
        print(self._tracker.chunkinfo())

    async def do_exit(self, arg):
        await self._tracker.stop()
        return True


class PeerTerminal(aiocmd.Cmd):
    INTRO = 'Welcome to \033[1mPeer\033[0m terminal.    Type help or ? to list commands.\n'
    PROMPT = 'peer > '

    def __init__(self, peer):
        assert isinstance(peer, Peer)
        self._peer = peer
        super().__init__()

    async def do_publish(self, arg):
        arg = arg.split(' ')[0]
        _, message = await self._peer.publish(arg)
        print(message)

    async def do_list_files(self, arg):
        file_list_dict = await self._peer.list_file()
        file_list = []
        headers = ['Filename']
        for filename, fileinfo in file_list_dict.items():
            if len(headers) == 1:
                headers.extend(tuple(map(lambda x: x.capitalize(), tuple(fileinfo.keys()))))
            file_list.append((filename,) + tuple(fileinfo.values()))

        print(tabulate(file_list, headers=headers))

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
            hook = tqdm_hook_wrapper(t)
            _, message = await self._peer.download(filename, destination, reporthook=hook)

        print(message)

    async def do_exit(self, arg):
        await self._peer.stop()
        return True
