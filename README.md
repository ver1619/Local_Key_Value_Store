# Local Key-Value Store 

A crash-safe, concurrent key-value store written in GO that demonstrates core database and storage-engine internals such as write-ahead logging (WAL), atomic persistence, crash recovery, secondary indexing, and a TCP server.<br>
This project is inspired by systems like Redis and LSM-tree-based databases like LevelDB and RocksDB, designed as a learning and demonstration project.

---

**Features:**
- In-memory key–value index (map[string]string)

- Write-Ahead Logging (append-only WAL)

- Atomic snapshot persistence

- Crash-safe recovery on restart

- Secondary index for prefix scans

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
**The serer listens on:**

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
PUT user:1 Joe
GET user:1
DELETE user:1
SAVE
EXIT
```

---

**Terminal Output:**

---

**Atmoic Snapshot (JSON):**

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







