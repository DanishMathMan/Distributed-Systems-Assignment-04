package main

import (
	proto "Distributed-Systems-Assignment-04/src/grpc"
	Node "Distributed-Systems-Assignment-04/src/server"
	"sync"

	"google.golang.org/grpc"
)

func main() {
	//make servers
	server1 := &Node.Node{
		Id: 1, Timestamp: make(chan int64, 1), State: Node.RELEASED,
	}
	server1.Timestamp <- 0

	server2 := &Node.Node{
		Id: 2, Timestamp: make(chan int64, 1), State: Node.RELEASED,
	}
	server2.Timestamp <- 0

	server3 := &Node.Node{
		Id: 3, Timestamp: make(chan int64, 1), State: Node.RELEASED,
	}
	server3.Timestamp <- 0

	//start servers
	wg := sync.WaitGroup{}
	wg.Go(func() {
		server1.StartServer(8080)
	})
	wg.Go(func() {
		server2.StartServer(8081)
	})
	wg.Go(func() {
		server3.StartServer(8082)
	})
	//start client connection (1,2), (1,3), (2,1), (2,3), (3,1), (3,2)
	//server1.CreateConnection(8080)
	conn1_2, err1 := grpc.NewClient("localhost:8081")
	conn1_3, err2 := grpc.NewClient("localhost:8082")
	conn2_1, err3 := grpc.NewClient("localhost:8080")
	conn2_3, err4 := grpc.NewClient("localhost:8082")
	conn3_1, err5 := grpc.NewClient("localhost:8080")
	conn3_2, err6 := grpc.NewClient("localhost:8081")

	client1_2 := proto.NewNodeClient(conn1_2)
	client1_3 := proto.NewNodeClient(conn1_3)
	client2_1 := proto.NewNodeClient(conn2_1)
	client2_3 := proto.NewNodeClient(conn2_3)
	client3_1 := proto.NewNodeClient(conn3_1)
	client3_2 := proto.NewNodeClient(conn3_2)

	wg.Wait()
}
