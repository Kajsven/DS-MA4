package main

import (
	pb "DS-MA4/grpc"
	"bufio"
	"context"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Node struct {
	pb.UnimplementedRAServiceServer
	id   int64
	time int64
	state string
	totalNodes int
	clientCons map[int64]*grpc.ClientConn
	request *Request
	mu sync.Mutex
}

type Request struct {
	time int64
	id  int64
	ackCount int
	done chan struct{}
}

var scanner = bufio.NewScanner(os.Stdin)

func newNode(id int64, totalNodes int) *Node {
	return &Node{id: id, time: 0, state: "RELEASED", totalNodes: totalNodes, request: nil}
}

func requestLessThan(req1 *Request, req2 *Request) bool {
	if req1.time < req2.time {
		return true
	} else if req1.time == req2.time {
		return req1.id < req2.id
	} else {
		return false
	}
}

func (n *Node) GetResource(ctx context.Context, req *pb.ResourceRequest) (*pb.ResourceResponse, error) {
	n.mu.Lock()
	n.time = max(n.time, req.Time) + 1
	n.mu.Unlock()

	if n.state == "HELD" || (n.state == "WANTED" && requestLessThan(n.request, &Request{time: req.Time, id: req.ProcessId})) {
		<-n.request.done
	}
	return &pb.ResourceResponse{}, nil
}


func createNode(id int64, totalNodes int, wg *sync.WaitGroup, ready *sync.WaitGroup) {
	defer wg.Done()
	var port string = fmt.Sprintf(":%v", 50000+id)
	lis, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}

	ready.Done()
	ready.Wait()

	n := newNode(id, totalNodes)
	
	n.createConnections()
	go n.ProcessLoop()
	
	srv := grpc.NewServer()
	pb.RegisterRAServiceServer(srv, n)

	log.Printf("Node %d listening on %s", id, port)
	if err := srv.Serve(lis); err != nil {
		log.Fatalf("serve: %v", err)
	}
}

func (n *Node) createConnections() {
	n.clientCons = make(map[int64]*grpc.ClientConn)

	for i := 0; i < n.totalNodes; i++ {
		if int64(i) != n.id {
			conn, err := grpc.NewClient(fmt.Sprintf(":%d", 50000+i), grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Fatalf("dial: %v", err)
			}
			n.clientCons[int64(i)] = conn
		}
	}
}

func (n *Node) sendRequest(targetId int64) {
	msg := pb.ResourceRequest{
		Time: n.time,
		ProcessId: n.id,
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	

	_, err := pb.NewRAServiceClient(n.clientCons[targetId]).GetResource(ctx, &msg)
	

	if err != nil {
		log.Printf("Error sending request to node %d: %v", targetId, err)
		return
	}

	n.request.ackCount++
}


func (n *Node) ProcessLoop() {
	for {
		log.Printf("Node %d state: %s, time: %d\n", n.id, n.state, n.time)
		time.Sleep(1 * time.Second)
		switch n.state {
		case "RELEASED":
			var randomInt int64 = rand.Int63n(int64(n.totalNodes))  //Random resource request interval
			if randomInt % int64(n.totalNodes) == n.id {
				n.state = "WANTED"
				n.time++
				n.request = &Request{
					time:     n.time,
					id:       n.id,
					ackCount: 0,
					done:     make(chan struct{}),
				}
				log.Printf("Node %d requesting resource at time %d\n", n.id, n.time)
				for i := 0; i < n.totalNodes; i++ {
					if int64(i) != n.id {
						go n.sendRequest(int64(i))
					}
				}
			}
		case "WANTED":
			if n.request.ackCount == n.totalNodes-1 {
				n.time++
				n.state = "HELD"
			}
		case "HELD":
			log.Printf("Node %d is in critical section\n", n.id)
			time.Sleep(2 * time.Second)
			n.time++
			n.state = "RELEASED"
			close(n.request.done)
			n.request = nil
		}
	}
}



