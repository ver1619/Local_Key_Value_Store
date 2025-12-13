package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"

	"local_kv/internal/kv"
)

const (
	dataDir  = "data"
	dataFile = dataDir + "/data.json"
	walFile  = dataDir + "/data.wal"
	addr     = ":4000"
)

func main() {
	store := kv.NewStore()

	// Startup recovery
	if _, err := os.Stat(dataFile); err == nil {
		if err := kv.Load(store, dataFile); err != nil {
			panic(err)
		}
	}
	if _, err := os.Stat(walFile); err == nil {
		if err := kv.ReplayWAL(store, walFile); err != nil {
			panic(err)
		}
	}

	wal, err := kv.OpenWAL(walFile)
	if err != nil {
		panic(err)
	}
	defer wal.Close()

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		panic(err)
	}
	fmt.Println("KV server listening on", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go handleClient(conn, store, wal)
	}
}

func handleClient(conn net.Conn, store *kv.Store, wal *kv.WAL) {
	defer conn.Close()

	in := bufio.NewScanner(conn)
	out := bufio.NewWriter(conn)
	defer out.Flush()

	for in.Scan() {
		line := strings.TrimSpace(in.Text())
		if line == "" {
			continue
		}

		resp := execute(line, store, wal)
		out.WriteString(resp + "\n")
		out.Flush()
	}
}

func execute(line string, store *kv.Store, wal *kv.WAL) string {
	parts := strings.Fields(line)
	cmd := strings.ToUpper(parts[0])

	switch cmd {

	case "PUT":
		if len(parts) < 3 {
			return "error: PUT <key> <value>"
		}
		key := parts[1]
		value := strings.Join(parts[2:], " ")

		if err := wal.AppendPut(key, value); err != nil {
			return "error: WAL failure"
		}
		store.Put(key, value)
		return "OK"

	case "GET":
		if len(parts) != 2 {
			return "error: GET <key>"
		}
		if v, ok := store.Get(parts[1]); ok {
			return v
		}
		return "not found"

	case "DELETE":
		if len(parts) != 2 {
			return "error: DELETE <key>"
		}
		if err := wal.AppendDelete(parts[1]); err != nil {
			return "error: WAL failure"
		}
		if store.Delete(parts[1]) {
			return "OK"
		}
		return "not found"

	case "SAVE":
		if err := kv.Save(store, dataFile); err != nil {
			return "error: save failed"
		}
		return "OK"

	default:
		return "error: unknown command"
	}
}
