package main

import (
	proto "Distributed-Systems-Assignment-04/src/grpc"
	queue "Distributed-Systems-Assignment-04/src/utility"
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
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
	Queue      queue.Queue
}

type State int

const (
	HELD     State = 1
	RELEASED State = 2
	WANTED   State = 3
)

// Request is the logic that happens within Node, when it receives a Request rpc call. To be called within Enter method
func (node *Node) Request(ctx context.Context, msg *proto.LamportMessage) (*proto.Empty, error) {
	//timestamp at the receival of a request
	timestamp := node.RemoteEvent(msg.LamportTimestamp)
	if node.State == HELD || (node.State == WANTED && !node.HasHigherPriority(msg)) {
		node.Queue.Enqueue(msg.GetProcessId())
	} else {
		//timestamp when replying to the message i.e. on method return
		timestamp = node.LocalEvent()
		node.OutClients[msg.GetProcessId()].Reply(ctx, &proto.LamportMessage{LamportTimestamp: timestamp, ProcessId: node.Id})
	}
	return nil, nil
}

// Reply is the logic that happens within the Node, when it receives a Reply rpc call. To be called within Request method
func (node *Node) Reply(ctx context.Context, msg *proto.LamportMessage) (*proto.Empty, error) {
	node.RemoteEvent(msg.LamportTimestamp)
	node.Replies[msg.ProcessId] <- true
	return nil, nil
}

func (node *Node) Enter() {
	node.State = WANTED
	//timestamp the sending of request messages. Note all messages have the same timestamp as sending this bundle of
	//requests is seen as one event.
	timestamp := node.LocalEvent()
	msg := &proto.LamportMessage{LamportTimestamp: timestamp, ProcessId: node.Id} //the message to be sent
	for _, client := range node.OutClients {
		client.Request(context.Background(), msg)
	}
	//wait for n-1 replies
	wg := sync.WaitGroup{}
	for _, v := range node.Replies {
		wg.Go(func() {
			<-v
		})
	}
	wg.Wait()
	//set state to held as the nodeServer has now gotten a reply from all other nodes' node which by Ricart-Agrawala
	//algorithm means only this process is in the critical section.
	node.State = HELD
	//call the critical section
	node.CriticalSection()
}

func (node *Node) Exit() {
	node.State = RELEASED
	timestamp := node.LocalEvent()
	msg := &proto.LamportMessage{LamportTimestamp: timestamp, ProcessId: node.Id}
	for {
		if node.Queue.IsEmpty() {
			return
		}
		client := node.OutClients[node.Queue.Dequeue()]
		client.Request(context.Background(), msg)
	}
}
func main() {
	//start server

	//input of command

	//-flag
	//--flag   // double dashes are also permitted
	//-flag=x

	port := flag.Int64("port", 8080, "Input port for the server to start on. Note, port is also its id")
	//id := flag.Int64("id", 0, "Id for the server to start on")
	flag.Parse()

	node := CreateNodeServer(*port)

	//do internal logic
	go func() {
		err := node.InputHandler()
		if err != nil {
			log.Println(err)
		}
	}()

	//todo internal logic. probably put some time.wait statements

	//TODO refactor
	for {
		//infinite loop to prevent premature return on main
	}
}

// InputHandler is meant to be called as a go routine, which will handle all CLI inputs, such as connecting to other servers
func (node *Node) InputHandler() error {
	//TODO connect to the other nodes
	//regex, _ := regexp.Compile(--connect *(?'port'\d{4})) //expect a four digit port number

	for {
		reader := bufio.NewReader(os.Stdin)
		msg, err := reader.ReadString('\n')
		if err != nil {
			return err
		}
		if strings.Contains(msg, "--connect") {

		}

	}
	return nil
}

func (node *Node) CriticalSection() {
	fmt.Printf("In critical section with client [%v]", node.Id)
	node.Exit()
}

func (node *Node) StartServer(port int64) {
	grpcServer := grpc.NewServer()
	lis, err := net.Listen("tcp", ":"+strconv.FormatInt(port, 10))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	proto.RegisterNodeServer(grpcServer, node)
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
		State:      RELEASED,
		Queue:      queue.Queue{}}
	return out
}

// ConnectToNodeServer connects this Node to another Node by creating a new connection and client to that
// Node's server
func (node *Node) ConnectToNodeServer(port int64) {
	node.LocalEvent()
	conn, err := grpc.NewClient("localhost:" + strconv.FormatInt(port, 10))
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	client := proto.NewNodeClient(conn)
	node.OutClients[port] = client
}

// LocalEvent updates the Timestamp by incrementing it by 1
func (node *Node) LocalEvent() int64 {
	newTimestamp := <-node.Timestamp + 1
	node.Timestamp <- newTimestamp
	return newTimestamp
}

// RemoteEvent updates the Timestamp by setting it to one greater than the maximum of the current Timestamp and the parameter provided timestamp
func (node *Node) RemoteEvent(timestamp int64) int64 {
	newTimestamp := max(<-node.Timestamp, timestamp) + 1
	node.Timestamp <- newTimestamp
	return newTimestamp
}

func (node *Node) HasHigherPriority(msg *proto.LamportMessage) bool {
	//get the timestamp of the node, without incrementing it
	timestamp := <-node.Timestamp
	node.Timestamp <- timestamp
	if timestamp < msg.GetLamportTimestamp() || (timestamp == msg.GetLamportTimestamp()) && node.Id < msg.ProcessId {
		return true
	}
	return false
}
