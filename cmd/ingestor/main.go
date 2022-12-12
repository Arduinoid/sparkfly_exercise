package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"
)

/*
Parallel Uniqueness Checker:
We have generated a number of CSV files containing
random strings that should be guaranteed to unique (e.g., X59J) within themselves and
among each other. Write code that can take the names of these files and verify the code
uniqueness across all CSV files in parallel. Bonus points if you can make it immediately
stop itself once it has found a duplicate code. Example CSV files can be found in the
attachments of this email.
*/

func main() {
	/*
		barcode,                        code,  YearWeek
		TestProcessEligibleChannel2_0,  HFGK,  TestProcessEligibleChannel2_0
	*/
	fmt.Println("ingestor for Sparkfly")
	// setup concurrency dependencies
	codesFound := new(sync.Map)
	dir, err := os.Open("./testdata")
	if err != nil {
		panic(err)
	}
	defer dir.Close()
	files, err := dir.ReadDir(-1)
	start := time.Now() // capture time to check for performance
	for _, f := range files {
		file, err := os.Open(dir.Name() + "/" + f.Name())
		defer file.Close()
		if err != nil {
			log.Printf("failed to open file: %s err: %s", f.Name(), err.Error())
			continue
		}
		err = readFile(file, codesFound)
		if err != nil && !errors.Is(err, io.EOF) {
			log.Printf("read file error: %s", err.Error())
			break
		}
	}
	total := time.Now().Sub(start) // evaluate total run time
	fmt.Printf("total time to run: %s", total.String())
}

func readFile(f *os.File, store *sync.Map) error {
	reader := csv.NewReader(f)
	var (
		readerErr        error
		firstLineSkipped bool
		line             []string
		recordCount      int
	)
	//reader.ReuseRecord = true

	for !errors.Is(readerErr, io.EOF) {
		if !firstLineSkipped {
			reader.Read()
			firstLineSkipped = true
			continue
		}
		line, readerErr = reader.Read()
		if readerErr != nil {
			if errors.Is(readerErr, io.EOF) {
				return readerErr
			} else {
				log.Printf("aborting file: %s, records processed: %d, error: %s", f.Name(), recordCount, readerErr.Error())
				return errors.New("reader error")
			}
		}
		recordCount++
		if len(line) == 0 {
			log.Printf("file: %s, no record data at offset: %d, at record: %d", f.Name(), reader.InputOffset(), recordCount)
			return errors.New("no data")
		}
		if _, ok := store.Load(line[1]); ok {
			log.Printf("aborting, duplicate found: %s", line[1])
			return errors.New("duplicate found")
		}
		store.Store(line[1], struct{}{})
	}
	return nil
}
