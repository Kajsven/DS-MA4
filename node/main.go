package main

import (
	"flag"
	"sync"
)


func main() {
	n := flag.Int("n", 3, "Number of nodes")
	flag.Parse()
	
	var wg = sync.WaitGroup{}
	for i := 0; i < *n; i++ {
		wg.Add(1)
		go createNode(int64(i), *n, &wg)
	}
	wg.Wait()
}