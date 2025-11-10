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

type Node struct {
	proto.UnimplementedNodeServer
	//OutClients are the clients this Node has to the other Nodes' servers.
	//Key corresponds to the port and id of the process the client connects to. This means lower port equals higher priority
	OutClients map[int64]proto.NodeClient
	Timestamp  chan int64
	Id         int64
	State      State
	Replies    map[int64]chan bool
	//TODO Queue
}

type State int

const (
	HELD     State = 1
	RELEASED State = 2
	WANTED   State = 3
)

// Request is the logic that happens within Node, when it receives a Request rpc call. To be called within Enter method
func (server *Node) Request(ctx context.Context, msg *proto.LamportMessage) (*proto.Empty, error) {
	//timestamp at the receival of a request
	timestamp := server.RemoteEvent(msg.LamportTimestamp)
	if server.State == HELD || (server.State == WANTED && !server.HasHigherPriority(msg)) {
		//TODO enqueue
	} else {
		//timestamp when replying to the message i.e. on method return
		timestamp = server.LocalEvent()
		server.OutClients[msg.GetProcessId()].Reply(ctx, &proto.LamportMessage{LamportTimestamp: timestamp, ProcessId: server.Id})
	}
	return nil, nil
}

// Reply is the logic that happens within the Node, when it receives a Reply rpc call. To be called within Request method
func (server *Node) Reply(ctx context.Context, msg *proto.LamportMessage) (*proto.Empty, error) {
	server.RemoteEvent(msg.LamportTimestamp)
	server.Replies[msg.ProcessId] <- true
	return nil, nil
}

func (server *Node) Enter() {
	server.State = WANTED
	//timestamp the sending of request messages. Note all messages have the same timestamp as sending this bundle of
	//requests is seen as one event.
	timestamp := server.LocalEvent()
	msg := &proto.LamportMessage{LamportTimestamp: timestamp, ProcessId: server.Id} //the message to be sent
	for _, client := range server.OutClients {
		client.Request(context.Background(), msg)
	}
	//wait for n-1 replies
	wg := sync.WaitGroup{}
	for _, v := range server.Replies {
		wg.Go(func() {
			<-v
		})
	}
	wg.Wait()
	//set state to held as the nodeServer has now gotten a reply from all other nodes' server which by Ricart-Agrawala
	//algorithm means only this process is in the critical section.
	server.State = HELD
}

func (server *Node) Exit() {
	server.State = RELEASED
	//TODO dequeue and reply to each until queue is empty
}
func main() {
	//start server

	//do internal logic
}
func (server *Node) StartServer(port int64) {
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

func CreateNodeServer(id int64) Node {
	out := Node{
		Id:         id,
		OutClients: make(map[int64]proto.NodeClient),
		Timestamp:  make(chan int64, 1),
		Replies:    make(map[int64]chan bool, 1),
		State:      RELEASED}
	return out
}

// ConnectToNodeServer connects this Node to another Node by creating a new connection and client to that
// Node's server
func (server *Node) ConnectToNodeServer(port int64) {
	server.LocalEvent()
	conn, err := grpc.NewClient("localhost:" + strconv.FormatInt(port, 10))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	client := proto.NewNodeClient(conn)
	server.OutClients[port] = client
}

// LocalEvent updates the Timestamp by incrementing it by 1
func (server *Node) LocalEvent() int64 {
	newTimestamp := <-server.Timestamp + 1
	server.Timestamp <- newTimestamp
	return newTimestamp
}

// RemoteEvent updates the Timestamp by setting it to one greater than the maximum of the current Timestamp and the parameter provided timestamp
func (server *Node) RemoteEvent(timestamp int64) int64 {
	newTimestamp := max(<-server.Timestamp, timestamp) + 1
	server.Timestamp <- newTimestamp
	return newTimestamp
}

func (server *Node) HasHigherPriority(msg *proto.LamportMessage) bool {
	//get the timestamp of the server, without incrementing it
	timestamp := <-server.Timestamp
	server.Timestamp <- timestamp
	if timestamp < msg.GetLamportTimestamp() || (timestamp == msg.GetLamportTimestamp()) && server.Id < msg.ProcessId {
		return true
	}
	return false
}
