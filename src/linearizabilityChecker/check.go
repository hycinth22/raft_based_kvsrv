package main

import (
	"fmt"
	"os"
	"time"
	"encoding/gob"

	"github.com/anishathalye/porcupine"

	"6.5840/models1"
)

const fpath string = "linearizability.html"
const timeout time.Duration = 600 * time.Second


func main() {
	gob.Register(porcupine.Operation{})
	gob.Register(models.KvInput{})
	gob.Register(models.KvOutput{})

	f, err := os.Open(os.Args[1])
	if err != nil {
		panic(err)
	}

	var opLog []porcupine.Operation
	dec := gob.NewDecoder(f)
	if err := dec.Decode(&opLog); err != nil {
		panic(err)
	}

	fmt.Println("start check input ", fpath)
	res, info := porcupine.CheckOperationsVerbose(models.KvModel, opLog, timeout)
	if res == porcupine.Illegal {
		fmt.Println("history is not linearizable")
	} else if res == porcupine.Unknown {
		fmt.Println("Unknown")
	} else {
		fmt.Println("history is linearizable")
	}

	var file *os.File
	if fpath == "" {
		// Save the vis file in a temporary file.
		file, err = os.CreateTemp("", "porcupine-*.html")
	} else {
		file, err = os.OpenFile(fpath, os.O_RDWR | os.O_CREATE | os.O_TRUNC, 0644)
	}
	err = porcupine.Visualize(models.KvModel, info, file)
	if err != nil {
		fmt.Printf("info: failed to write history visualization to %s\n", file.Name())
	} else {
		fmt.Printf("info: wrote history visualization to %s\n", file.Name())
	}
}