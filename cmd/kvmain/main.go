package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"local_kv/internal/kv"
)

const (
	dataDir  = "data"
	dataFile = dataDir + "/data.json"
	walFile  = dataDir + "/data.wal"
)

type Command struct {
	Name string
	Args []string
}

func parseCommand(line string) (*Command, error) {
	line = strings.TrimSpace(line)
	if line == "" {
		return nil, fmt.Errorf("empty command")
	}

	parts := strings.Fields(line)
	name := strings.ToUpper(parts[0])

	return &Command{
		Name: name,
		Args: parts[1:],
	}, nil
}

/*
enforces argument correctness.
*/
func validateCommand(c *Command) error {
	switch c.Name {

	case "PUT", "SET":
		if len(c.Args) < 2 {
			return fmt.Errorf("PUT requires at least 2 arguments: PUT <key> <value>")
		}

	case "GET":
		if len(c.Args) != 1 {
			return fmt.Errorf("GET requires exactly 1 argument: GET <key>")
		}

	case "DELETE", "DEL", "RM":
		if len(c.Args) != 1 {
			return fmt.Errorf("DELETE requires exactly 1 argument: DELETE <key>")
		}

	case "SAVE", "LOAD", "HELP", "EXIT", "QUIT", "Q":
		if len(c.Args) != 0 {
			return fmt.Errorf("%s takes no arguments", c.Name)
		}

	default:
		return fmt.Errorf("unknown command: %s (type HELP)", c.Name)
	}
	return nil
}

/*
Supported commands.
*/
func printHelp() {
	fmt.Println(`
Available Commands:

  PUT <key> <value>      Store or overwrite a key-value pair
  SET <key> <value>      Alias for PUT

  GET <key>              Retrieve a value

  DELETE <key>           Delete a key
  DEL <key>              Alias for DELETE
  RM <key>               Alias for DELETE

  SAVE                   Persist snapshot and reset WAL
  LOAD                   Reload snapshot (does NOT clear WAL)

  HELP                   Show this help message

  EXIT | QUIT | Q        Exit the program
`)
}

func main() {
	store := kv.NewStore()

	/* ---------------- Startup Recovery ---------------- */

	// 1. Load snapshot if present
	if _, err := os.Stat(dataFile); err == nil {
		if err := kv.Load(store, dataFile); err != nil {
			fmt.Println("fatal: failed to load snapshot:", err)
			return
		}
		fmt.Println("snapshot loaded")
	} else if !os.IsNotExist(err) {
		fmt.Println("fatal: error checking snapshot:", err)
		return
	}

	// 2. Replay WAL if present
	if _, err := os.Stat(walFile); err == nil {
		if err := kv.ReplayWAL(store, walFile); err != nil {
			fmt.Println("fatal: failed to replay WAL:", err)
			return
		}
		fmt.Println("WAL replayed")
	}

	// 3. Open WAL for appends
	wal, err := kv.OpenWAL(walFile)
	if err != nil {
		fmt.Println("fatal: cannot open WAL:", err)
		return
	}
	defer wal.Close()

	/* ---------------- REPL ---------------- */

	fmt.Println("local-kv REPL (type HELP for commands)")
	in := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("> ")
		if !in.Scan() {
			fmt.Println("\nexiting")
			return
		}

		cmd, err := parseCommand(in.Text())
		if err != nil {
			fmt.Println("error:", err)
			continue
		}

		if err := validateCommand(cmd); err != nil {
			fmt.Println("error:", err)
			continue
		}

		switch cmd.Name {

		/* -------- PUT / SET -------- */
		case "PUT", "SET":
			key := cmd.Args[0]
			value := strings.Join(cmd.Args[1:], " ")

			if err := wal.AppendPut(key, value); err != nil {
				fmt.Println("WAL error:", err)
				continue
			}

			store.Put(key, value)
			fmt.Println("OK")

		/* -------- GET -------- */
		case "GET":
			if v, ok := store.Get(cmd.Args[0]); ok {
				fmt.Println(v)
			} else {
				fmt.Println("not found")
			}

		/* -------- DELETE -------- */
		case "DELETE", "DEL", "RM":
			key := cmd.Args[0]

			if err := wal.AppendDelete(key); err != nil {
				fmt.Println("WAL error:", err)
				continue
			}

			if store.Delete(key) {
				fmt.Println("OK")
			} else {
				fmt.Println("not found")
			}

		/* -------- SAVE (Snapshot + WAL Reset) -------- */
		case "SAVE":
			if err := kv.Save(store, dataFile); err != nil {
				fmt.Println("save error:", err)
				continue
			}

			// WAL compaction
			wal.Close()
			os.Remove(walFile)
			wal, _ = kv.OpenWAL(walFile)

			fmt.Println("snapshot saved; WAL reset")

		/* -------- LOAD (Manual Reload) -------- */
		case "LOAD":
			if err := kv.Load(store, dataFile); err != nil {
				fmt.Println("load error:", err)
			} else {
				fmt.Println("snapshot loaded")
			}

		/* -------- HELP -------- */
		case "HELP":
			printHelp()

		/* -------- EXIT -------- */
		case "EXIT", "QUIT", "Q":
			fmt.Println("goodbye")
			return
		}
	}
}
