# p2pfs
[![Build Status](https://travis-ci.com/RyanWangGit/p2pfs.svg?token=6D8zTzZr7SPui6PzhT2a&branch=master)](https://travis-ci.com/RyanWangGit/p2pfs) [![codecov](https://codecov.io/gh/RyanWangGit/p2pfs/branch/master/graph/badge.svg?token=EDGIegqh8K)](https://codecov.io/gh/RyanWangGit/p2pfs)

<p align="center">
  <img src="https://ryanwanggit.github.io/p2pfs/demo.svg" />
</p>

File System based on P2P.

The system include multiple clients (also named peers) and one central server. A peer can
join the P2P file sharing system by connecting to the server and providing a list of files it wants to
share. The server shall keep a list of all the files shared on the network. The file being distributed is
divided into chunks (e.g. a 10 MB file may be transmitted as ten 1 MB chunks). For each file, the
server shall be responsible for keeping track of the list of chunks each peer has. As a peer receives
a new chunk of the file it becomes a source (of that chunk) for other peers. When a peer intends
to download a file, it will initiate a direct connection to the relevant peers to download the file. A
peer should be able to download different chunks of the file simultaneously from many peers.

Both peer and tracker acts as server **and** client. For a tracker, it only listens for peers to connect to acquire informatoin about the current files and the peers who have the file. For a peer, it connects to tracker to get the information and connects to other peers to get the data. The connections between tracker and peer are always alive, while the connections between peers are disconnected when the file transfer is done.

When downloading, peers will ping other peers to start seeking content from the lowest-latency-peer, note that currently parallel download is not implemented.

The system is designed to be as fail-safe as possible, i.e., peers can shutdown abruptly at any time (during file transfer/when idling, etc.), and trackers can shutdown abruptly at any time, too. The list below shows what the system does when unexpected shutdown happens:

* **Tracker shutdown**: Peers won't be able to retieve file/peers information until the it connects to the tracker again (need to manually use the command `connect <tracker_ip> <tracker_port>`). Current file transfers won't be affected (as the peers information has already been downloaded).

* **Peer shutdown**: Current file transfers which the crashed peer doesn't have won't be affected (of course!), for the ones that the crashed peer is involved, the downloading peer will seek other peers for the content. If no other peers have the file the downloading peer will report an error for incomplete tranfer and the tracker will delist the file on its record. 

## Tracker Commands
* `start <host> <port>`

Start listening on `<host>:<port>` for peer connections.

* `list_peers`

Print the currently-connected  peers, this is for debug purposes.

* `exit`

Shutdown the tracker.

## Peer Commands
* `connect <trackerip> <trackerport>`

The peer intents to join the system.  It will send a message of type `REQUEST_REGISTER` to the tracker and wait for a message of type `REPLY_REGISTER`.

* `publish <localfile>`

The peer offers to share a file.  It will send a messageof type `REQUEST_PUBLISH` to the tracker and wait for amessage of type `REPLY_PUBLISH`.

* `list_file`

The peer wishes to know what files are available. It sends a message of type `REQUEST_FILE_LIST` to the tracker and waits for a message of type `REPLY_FILE_LIST`.

* `download <remote_file> <local_path>`

The peer wishes download a file. It first sends a message of type `REQUEST_FILE_LOCATION` to the tracker and waits for a message of type `REPLY_FILE_LOCATION`. After receiving this message, the peer will know which peer has which chunks of the file. It will decide which chunks to download first according to the rarest-chunk-first rule. Then it will send a ping (`PEER_PING_PONG`) to all peers that has the chunks to determine the latency (round trip time), and choose the ones with small RTT for downloading. It sends a message of type `PEER_REQUEST_CHUNK` to those peers and wait for a `PEER_REPLY_CHUNK` message. After receiving the chunk, it then sends a `REQUEST_CHUNK_REGISTER` message to the tracker to inform the tracker that it now also has this chunk (and therefore can be a source for this chunk).

* `exit`

Shutdown the peer.

## Message Passing Protocol
We use `JSON` to transfer messages between tracker/peer and peer/peer. Each message is a `JSON` dict and has a `type` field, messages with different types have different fields which are listed as follows:


| Type                   | Field                                                                                                                                                                     |
|------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| REQUEST_REGISTER       | `address`: Peer's server address                                                                                                                                          |
| REPLY_REGISTER         | None                                                                                                                                                                      |
| REQUEST_PUBLISH        | `filename`: the name of the file<br /> `fileinfo`: (`filesize`, `totalchunknum`)                                                                                          |
| REPLY_PUBLISH          | `result`: True for success, false for failure<br /> `message`: The reason for the failure                                                                                 |
| REQUEST_FILE_LIST      | None                                                                                                                                                                      |
| REPLY_FILE_LIST        | `filelist`: {`filename` -> (`filesize`, `totalchunknum`)}                                                                                                                 |
| REQUEST_FILE_LOCATION  | `filename`: Name of the file                                                                                                                                              |
| REPLY_FILE_LOCATION    | `filename`: Name of the file<br /> `chunkinfo`: The location information of each chunk of the file                                                                        |
| REQUEST_CHUNK_REGISTER | `filename`: Name of the file<br /> `chunknum`: Number of the chunk to register                                                                                            |
| PEERREQUEST_CHUNK      | `filename`: Name of the file<br /> `chunknum`: Chunk number of the file                                                                                                   |
| PEER_REPLY_CHUNK       | `filename`: Name of the file<br /> `chunknum`: Chunk number of the file<br /> `data`: BASE64 encoding of the binary data<br /> `digest`: SHA256 digest of the binary data |
| PEER_PING_PONG         | None. (This is to test peers RTT, the peer will send it back once it receives a PEER_PING_PONG message) 

## Tracker / Peer Behaviors
### Tracker
* `REQUEST_REGISTER`

The tracker sees that a peer attempts to join the system.It adds the peer to its list of peers and sends back a message of type `REPLY_REGISTER`.

* `REQUEST_PUBLISH`

The tracker sees that a peer attempts to share a file.  Ifthis file is not present on its list of files, it adds the fileto the list and update the chunk info on this file.  Thetracker sends back a message of type `REPLY_PUBLISH`.

* `REQUEST_FILE_LIST`

The tracker sees that a peer wants to get a list of filescurrently shared in the system. It sends back a message of type `REPLY_FILE_LIST` containing this list.

* `REQUEST_FILE_LOCATION`

The tracker sees that a peer wants to know which peerhas which chunks about a file. It sends back a messageof  type `REPLY_FILE_LOCATION `containing this information.

* `REQUEST_CHUNK_REGISTER`

When a peer has successfully downloaded a chunk of a file, it will send a message of type `REQUEST_CHUNK_REGISTER` to the tracker. The tracker therefore knows that this peer also has a copy of this chuck of a file. It will update the chunk info accordingly.

### Peer
* `PEER_REQUEST_CHUNK`

Peer reads the chunk of the file and sends it back.

* `PEER_PING_PONG`

Peer sends back the exact message.
