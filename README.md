# Extended TFTP Client–Server System (Java)

## Overview
This project is a full implementation of an Extended TFTP protocol running over TCP, with a standalone server and a threaded client. It provides a binary, block-based file transfer protocol with acknowledgments, directory listing, deletion, and server broadcast notifications.

From a systems and networking perspective, the code showcases binary protocol design, framing/encoding, stateful transfers, and concurrency across multiple clients. The protocol logic is implemented as a state machine, ensuring correctness under partial reads and out-of-order or duplicate packets.

The system was originally built in an academic setting, but has been upgraded into a production-quality networking project with deterministic framing, robust error handling, and automated tests. It is structured for readability and extension, with clear separation between protocol logic, I/O, and file storage.

## Features
- Binary protocol implementation
- Big-endian encoding/decoding
- Multi-block file transfers (DATA / ACK)
- Concurrent clients support
- Threaded client (keyboard + listener)
- Server-side broadcast (BCAST)
- Robust error handling
- Clean protocol state machine
- Automated tests

## Protocol Summary
- **Supported opcodes**: LOGRQ, RRQ, WRQ, DIRQ, DELRQ, DISC, DATA, ACK, BCAST, ERROR
- **Transfers**: Block-based, one block per ACK
- **Termination**: DATA packet size < 512 bytes indicates transfer completion
- **Broadcast**: Server notifies logged-in clients on file add/delete

## Architecture
Client:
- Two threads: keyboard input thread for outbound commands, listener thread for inbound packets.
- Protocol layer converts received packets into actions and responses.

Server:
- Thread-per-connection handler with a shared connections registry.
- Protocol state machine per connection, isolated from other clients.
- FileStore service encapsulates all filesystem operations under `server/Files`.

Encoder/Decoder:
- Deterministic byte-stream framing per opcode.
- Produces complete packets only when fully received.

ASCII sketch:
```
Client           TCP            Server
------                          -------
Keyboard ---> [Protocol] ---> [Enc/Dec] ---> [Protocol] ---> FileStore
Listener <--- [Protocol] <--- [Enc/Dec] <--- [Protocol] <--- Connections
```

## Project Structure
- `client/` – Client implementation, protocol logic, and encoder/decoder.
- `server/` – Server implementation, protocol logic, connections, and file store.
- `server/Files/` – Server-side storage root (created if missing).
- `README.md` – Project overview and usage.

## How to Build
```bash
cd server
mvn clean compile

cd ../client
mvn clean compile
```

## How to Run
Server:
```bash
cd server
mvn exec:java -Dexec.mainClass="bgu.spl.net.impl.tftp.TftpServer" -Dexec.args="7777"
```

Client:
```bash
cd client
mvn exec:java -Dexec.mainClass="bgu.spl.net.impl.tftp.TftpClient" -Dexec.args="127.0.0.1 7777"
```

## Example Client Session
```
LOGRQ alice
RRQ small.txt
DIRQ
WRQ big.bin
DELRQ old.txt
DISC
```

## Testing
```bash
cd server
mvn test

cd ../client
mvn test
```
