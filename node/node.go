package main

import (
	pb "DS-MA4/grpc"
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
)

type Node struct {
	pb.UnimplementedRAServiceServer
	id   int64
	time int64
	state string
	totalNodes int
}

type Request struct {
	time int64
	id  int64
	done chan struct{}
}

var scanner = bufio.NewScanner(os.Stdin)

func newNode(id int64, totalNodes int) *Node {
	return &Node{id: id, time: 0, state: "RELEASED", totalNodes: totalNodes}
}


func createNode(id int64, totalNodes int, wg *sync.WaitGroup) {
	defer wg.Done()
	var port string = fmt.Sprintf(":%v", 50000+id)
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	n := newNode(id, totalNodes)
	go n.ProcessLoop()

	srv := grpc.NewServer()
	pb.RegisterRAServiceServer(srv, n)

	log.Printf("Node %d listening on %s", id, port)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func (n *Node) ProcessLoop() {
	for {
		time.Sleep(1 * time.Second)
		switch n.state {
		case "RELEASED":
			var randomInt int64 = rand.Int63n(int64(n.totalNodes))  //Random resource request interval
			if randomInt % int64(n.totalNodes) == n.id {
				n.state = "WANTED"
				for i := 0; i < n.totalNodes; i++ {
					if int64(i) != n.id {
						msg := pb.ResourceRequest{
							Time: n.time,
							ProcessId: n.id,
						}
						
					}
				}
			}
		}
		
		
		if n.state == "RELEASED" {

			// ask for resource
			// set state wanted
			// multi cast

		} else if n.state == "WANTED" {
			// wait for replies
			if request.count == 0 {
				// use resource
				// close chan
			}

		}
		
	}
}



