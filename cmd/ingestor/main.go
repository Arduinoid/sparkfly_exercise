package main

import (
	"context"
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
	ctx, cf := context.WithCancel(context.Background()) // used to drive short circuit behavior
	wg := new(sync.WaitGroup)
	codesFound := new(sync.Map)
	dir, err := os.Open("./testdata")
	if err != nil {
		panic(err)
	}
	defer dir.Close()
	files, err := dir.ReadDir(-1)
	start := time.Now()
	for _, f := range files {
		file, err := os.Open(dir.Name() + "/" + f.Name())
		defer file.Close()
		if err != nil {
			log.Printf("failed to open file: %s err: %s", f.Name(), err.Error())
			continue
		}
		fmt.Printf("launch go routine for file: %s\n", file.Name())
		wg.Add(1)
		go readFile(ctx, cf, file, wg, codesFound)
	}
	wg.Wait()
	total := time.Now().Sub(start)
	fmt.Printf("total run time: %s\n", total.String())
}

func readFile(ctx context.Context, cf context.CancelFunc, f *os.File, wg *sync.WaitGroup, store *sync.Map) {
	defer func() {
		if err := recover(); err != nil {
			log.Printf("go routine paniced, file: %s error: %s", f.Name(), err)
		}
	}()
	defer wg.Done()
	reader := csv.NewReader(f)
	reader.ReuseRecord = true
	var readerErr error
	var firstLineSkipped bool

	for !errors.Is(readerErr, io.EOF) {
		select {
		case <-ctx.Done():
			log.Println("aborting context finished")
			return
		default:
			if !firstLineSkipped {
				reader.Read()
				firstLineSkipped = true
				continue
			}
			line, err := reader.Read()
			if err != nil {
				log.Printf("aborting file: %s, could not read line: %s", f.Name(), err.Error())
				cf()
				return
			}
			if len(line) == 0 {
				log.Printf("record contains no data at offset: %d", reader.InputOffset())
				return
			}
			if _, ok := store.Load(line[1]); ok {
				log.Printf("aborting, duplicate found: %s", line[1])
				cf()
				return
			}
			store.Store(line[1], struct{}{})
		}
	}
}
