package main

import "fmt"

/*
Basic Byte Compressor:
We receive large (20MB+) text files that have to be stored in S3
for safe keeping. To minimize costs, we would like to store them in a compressed
format. To further minimize costs, we would like to offload this process onto lowmemory hardware. We get these files regularly and need the software that processes
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
	fmt.Println("compressor fot sparkfly")
}
