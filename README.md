# p2pfs
[![Build Status](https://travis-ci.com/RyanWangGit/p2pfs.svg?token=6D8zTzZr7SPui6PzhT2a&branch=master)](https://travis-ci.com/RyanWangGit/p2pfs) [![codecov](https://codecov.io/gh/RyanWangGit/p2pfs/branch/master/graph/badge.svg?token=EDGIegqh8K)](https://codecov.io/gh/RyanWangGit/p2pfs)

File System based on P2P.

The system include multiple clients (also named peers) and one central server. A peer can
join the P2P file sharing system by connecting to the server and providing a list of files it wants to
share. The server shall keep a list of all the files shared on the network. The file being distributed is
divided into chunks (e.g. a 10 MB file may be transmitted as ten 1 MB chunks). For each file, the
server shall be responsible for keeping track of the list of chunks each peer has. As a peer receives
a new chunk of the file it becomes a source (of that chunk) for other peers. When a peer intends
to download a file, it will initiate a direct connection to the relevant peers to download the file. A
peer should be able to download different chunks of the file simultaneously from many peers.
