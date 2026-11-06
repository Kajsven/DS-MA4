package main

import (
	"flag"
	"sync"
)


func main() {
	n := flag.Int("n", 3, "Number of nodes")
	flag.Parse()
	
	var wg = sync.WaitGroup{}
	var ready = sync.WaitGroup{}
	ready.Add(*n)
	wg.Add(*n)

	for i := 0; i < *n; i++ {
		go createNode(int64(i), *n, &wg, &ready)
	}

	wg.Wait()
}