# Sparkfly Interview Exercise

## Concurrency Exercise

ingest each CSV file and process them in parallel ensuring uniqueness in each file and between files.
Bonus for making the system stop upon finding a reoccurring string

## Data Compression Exercise

compress an incoming stream of bytes using GZIP in a memory efficient way. Solution must take an `io.ReaderCloser` 
and return `io.Reader`
