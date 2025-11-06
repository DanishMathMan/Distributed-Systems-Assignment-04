package node

import (
	proto "Distributed-Systems-Assignment-04/src/grpc"
	"context"
	"log"
	"net"
	"strconv"
	"sync"

	"google.golang.org/grpc"
)

type NodeServer struct {
	proto.UnimplementedNodeServer
	OutClients map[int64]proto.NodeClient //OutClients are the clients this Node has to the other Nodes' servers.
	Timestamp  chan int64
	Id         int64
	State      State
	Replies    map[int64]chan bool
}

type State int

const (
	HELD     State = 1
	RELEASED State = 2
	WANTED   State = 3
)

// Request is the logic that happens within NodeServer, when it receives a Request rpc call. To be called within Enter method
func (server *NodeServer) Request(ctx context.Context, msg *proto.LamportMessage) (*proto.Empty, error) {
	timestamp := server.RemoteEvent(msg.LamportTimestamp)
	if server.State == HELD || (server.State == WANTED && timestamp < msg.GetLamportTimestamp()) {

	} else {
		server.Reply(ctx, &proto.LamportMessage{LamportTimestamp: server.LocalEvent(), ProcessId: server.Id})
	}
	return nil, nil
}

// Reply is the logic that happens within the NodeServer, when it receives a Reply rpc call. To be called within Request method
func (server *NodeServer) Reply(ctx context.Context, msg *proto.LamportMessage) (*proto.Empty, error) {
	server.RemoteEvent(msg.LamportTimestamp)
	server.Replies[msg.ProcessId] <- true
	return nil, nil
}

func (server *NodeServer) Enter() {
	server.LocalEvent()
	server.State = WANTED
	wg := sync.WaitGroup{}
	for _, v := range server.OutClients {
		msg := &proto.LamportMessage{}
		wg.Go(func() {
			v.Request(context.Background(), msg)
		})

	}
	//wait for n-1 replies
	for _, v := range server.Replies {
		<-v
	}
	//set state
	server.State = HELD
}
func main() {
	//start server

	//do internal logic
}
func (server *NodeServer) StartServer(port int64) {
	grpcServer := grpc.NewServer()
	lis, err := net.Listen("tcp", ":"+strconv.FormatInt(port, 10))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	proto.RegisterNodeServer(grpcServer, server)
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func CreateNodeServer(id int64) NodeServer {
	out := NodeServer{
		Id:         id,
		OutClients: make(map[int64]proto.NodeClient),
		Timestamp:  make(chan int64, 1),
		Replies:    make(map[int64]chan bool, 1),
		State:      RELEASED}
	return out
}

// ConnectToNodeServer connects this NodeServer to another NodeServer by creating a new connection and client to that
// NodeServer's server
func (server *NodeServer) ConnectToNodeServer(port int64) {
	server.LocalEvent()
	conn, err := grpc.NewClient("localhost:" + strconv.FormatInt(port, 10))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	client := proto.NewNodeClient(conn)
	server.OutClients[port] = client
}

// LocalEvent updates the Timestamp by incrementing it by 1
func (server *NodeServer) LocalEvent() int64 {
	newTimestamp := <-server.Timestamp + 1
	server.Timestamp <- newTimestamp
	return newTimestamp
}

// RemoteEvent updates the Timestamp by setting it to one greater than the maximum of the current Timestamp and the parameter provided timestamp
func (server *NodeServer) RemoteEvent(timestamp int64) int64 {
	newTimestamp := max(<-server.Timestamp, timestamp) + 1
	server.Timestamp <- newTimestamp
	return newTimestamp
}
