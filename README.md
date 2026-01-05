# *Local Key-Value Store v1*

A crash-safe, concurrent key-value store written in GO that demonstrates core database and storage-engine internals such as write-ahead logging (WAL), atomic persistence, crash recovery.<br>
This project is inspired by basic concepts used in systems like Redis and LSM-tree-based databases, designed as a learning and demonstration project.

---

```
## Project Structure

cmd/
  kvmain/      # CLI (single client)
  kvserver/    # TCP server (multi-client)
internal/kv/   # Core storage engine (Store, WAL, Snapshot)
data/          # Runtime data (snapshot + WAL)
```
**CLI vs TCP Server**

- CLI mode is for single-user, local interaction.
- TCP server mode allows multiple clients to connect concurrently over the network.
- Both modes use the same storage engine, WAL, and snapshot logic.


**NOTE : Create a directory named data/ in the root directory before running the program**
- WAL grows until a snapshot (SAVE) is taken.
- In CLI mode, SAVE resets the WAL.
- In TCP server mode, WAL compaction is not implemented.

**Requirements**
- Install go 1.22+

**Features:**
- In-memory key–value index (map[string]string)

- Write-Ahead Logging (append-only WAL)

- Atomic snapshot persistence

- Crash-safe recovery on restart

- Interactive CLI

- Concurrent TCP server

---

**Operations:**

- Store data<br>
- Fetch data<br>
- Persist/Save data<br>
- Delete data<br>
- Exit the program<br>

---

**How to run:**

**1) For main:( Single Client )**

```bash
go run ./cmd/kvmain
```

**2) For tcp server:( Multi Client )**
```bash
go run ./cmd/kvserver
```
**The server listens on:**

`localhost:4000`

**Open another terminal and run:**

```bash
nc localhost 4000
```


**NOTE : Run any one ( main or tcp server ), avoid running both at the same time**


**3) CLI commands:**

```bash
PUT <key> <value>
GET <key>
DELETE <key>
SAVE
HELP
EXIT
```

**Example:**

```bash
PUT user:Joe
GET user
DELETE user
SAVE
EXIT
```

---

**Terminal Output:**

<img width="400" height="473" alt="terminal" src="https://github.com/user-attachments/assets/6caf5a62-c521-4851-b009-fdd3695bf5a2" />

---

**Atomic Snapshot (JSON):**

```json
{
    "user:1": "Joe"
}
```

**WAL (Append-only):**

```bash
{"op":"PUT","key":"user:1","value":"Joe"}
{"op":"PUT","key":"role","value":"student"}
{"op":"DELETE","key":"role"}
```







