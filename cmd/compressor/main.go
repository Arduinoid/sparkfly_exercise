package main

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"runtime"
)

/*
Basic Byte Compressor:
We receive large (20MB+) text files that have to be stored in S3
for safe keeping. To minimize costs, we would like to store them in a compressed
format. To further minimize costs, we would like to offload this process onto low memory hardware. We get these files regularly and need the software that processes
them to be expedient. For simplicity, we have decided to use the GZIP compression
format as it offers the balance between speed/compression that we need. Please write
code that takes uncompressed input and writes compressed output. The interface
requirements are:
a. The upload manager to S3 takes an io.Reader as its argument (output from your
code)
b. The uncompressed data is provided to your code as an io.ReadCloser (input to
your code)
Here is an example of the interface your code should satisfy:
// YourSolution is an io.Reader that provides compressed bytes
type YourSolution interface {
io.Reader
}
// NewYourSolution takes a source of uncompressed bytes
func NewYourSolution(rc io.ReadCloser) *YourSolution {
// instantiation code here
}
*/

func main() {
	fmt.Println("compressor for sparkfly")
	data := &myString{}
	data.Write([]byte("hello to all ya'll good people"))
	comp := compress(data)
	fmt.Printf("%v\n", comp)
	res, err := decompress(comp)
	if err != nil {
		panic(err)
	}
	fmt.Printf("%s\n", res)
	fmt.Println("end program...")
}

func compress(in io.ReadCloser) io.Reader {
	defer in.Close()
	out := &bytes.Buffer{}
	gz := gzip.NewWriter(out)
	defer gz.Close()
	io.Copy(gz, in)
	return out
}

func decompress(in io.Reader) (string, error) {
	zr, err := gzip.NewReader(in)
	if err != nil {
		return "", err
	}
	out := &myString{}
	io.Copy(out, zr)
	return out.String(), nil
}

type myString struct {
	bytes.Buffer
}

func (m *myString) Close() error {
	return nil
}

func sumTotalMemoryUsed(m *runtime.MemStats) uint64 {
	return m.HeapSys + m.StackSys + m.OtherSys
}
