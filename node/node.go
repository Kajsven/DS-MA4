package main

import (
	pb "DS-MA4/grpc"
)

type Node struct {
	pb.UnimplementedRAServiceServer
	id   int64
	time int64
}

