package main

import (
	proto "Distributed-Systems-Assignment-04/src/grpc"
	queue "Distributed-Systems-Assignment-04/src/utility"
	utility "Distributed-Systems-Assignment-04/src/utility"
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
	Start      chan bool
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
		_, err := node.OutClients[msg.GetProcessId()].Reply(ctx, &proto.LamportMessage{LamportTimestamp: timestamp, ProcessId: node.Id})
		if err != nil {
			log.Fatalln(err)
			return nil, err
		}
	}
	return &proto.Empty{}, nil
}

// Reply is the logic that happens within the Node, when it receives a Reply rpc call. To be called within Request method
func (node *Node) Reply(ctx context.Context, msg *proto.LamportMessage) (*proto.Empty, error) {
	node.RemoteEvent(msg.LamportTimestamp)
	node.Replies[msg.ProcessId] <- true
	return &proto.Empty{}, nil
}

/*
Ping exists to make sure that when the Node creates a new client connection, that the server it connects to exists by
calling this rpc method to that server. If the server doesn't the rpc call to this method will return an error, indicating
that the port doesn't have a listening serve Node server.
*/
func (node *Node) Ping(ctx context.Context, msg *proto.Empty) (*proto.Empty, error) {
	return &proto.Empty{}, nil
}

/*
Enter is called when the Node wants to Access the critical section, calling the Request rpc to every other Node and awaiting
a reply from each, before using the critical section.
*/
func (node *Node) Enter() {
	node.State = WANTED
	//timestamp the sending of request messages. Note all messages have the same timestamp as sending this bundle of
	//requests is seen as one event.
	timestamp := node.LocalEvent()
	msg := &proto.LamportMessage{LamportTimestamp: timestamp, ProcessId: node.Id} //the message to be sent
	for _, client := range node.OutClients {
		_, err := client.Request(context.Background(), msg)
		if err != nil {
			return
		}
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
	port := flag.Int64("port", 8080, "Input port for the server to start on. Note, port is also its id")
	flag.Parse()

	// implemented logger
	err := os.Mkdir("NodeLogs", 0750)
	if err != nil && !os.IsExist(err) {
		log.Fatal(err)
	}
	f, err := os.OpenFile("NodeLogs/log_"+strconv.FormatInt(time.Now().Unix(), 10)+".json", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		log.Fatal(err)
	}
	log.SetFlags(0)
	log.SetOutput(f)

	node := CreateNodeServer(*port)
	go node.StartServer(node.Id)
	//do internal logic
	go func() {
		err := node.InputHandler()
		if err != nil {
			log.Println(err)
		}
	}()

	go func() {
		<-node.Start
		for {
			// TODO change >= to >
			if len(node.OutClients) >= 1 {
				time.Sleep(1 * time.Second)
				node.Enter()
			}
		}
	}()

	//TODO refactor
	for {
		//infinite loop to prevent premature return on main
	}
}

// InputHandler is meant to be called as a go routine, which will handle all CLI inputs, such as connecting to other servers
func (node *Node) InputHandler() error {
	//TODO connect to the other nodes
	regex := regexp.MustCompile("(?:--connect|-c) *(?P<port>\\d{4})") //expect a four digit port number
	regex2 := regexp.MustCompile("--start")

	for {
		reader := bufio.NewReader(os.Stdin)
		msg, err := reader.ReadString('\n')
		if err != nil {
			log.Println(err)
			continue
		}
		match := regex.FindStringSubmatch(msg)
		match2 := regex2.FindStringSubmatch(msg)
		if match == nil && match2 == nil {
			log.Println("Invalid input: " + strings.Split(msg, "\n")[0])
			continue
		}

		if match2 != nil {
			node.Start <- true
			continue
		}

		port, _ := strconv.ParseInt(match[1], 10, 64)
		err = node.ConnectToNodeServer(port)
		if err != nil {
			log.Println(err)
			continue
		}
	}
}

func (node *Node) CriticalSection() {
	timestamp := node.LocalEvent()
	fmt.Printf("In critical section with client [%v] at timestamp %v\n", node.Id, timestamp)
	utility.LogAsJson(utility.LogStruct{Timestamp: timestamp, Identifier: node.Id}, true)
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
		Queue:      queue.Queue{},
		Start:      make(chan bool)}
	out.Timestamp <- 0
	return out
}

// ConnectToNodeServer connects this Node to another Node by creating a new connection and client to that
// Node's server
func (node *Node) ConnectToNodeServer(port int64) error {
	if node.OutClients[port] != nil {
		return errors.New("Already connected to port: " + strconv.FormatInt(port, 10))
	}

	node.LocalEvent()
	conn, err := grpc.NewClient("localhost:"+strconv.FormatInt(port, 10), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	client := proto.NewNodeClient(conn)
	_, err = client.Ping(context.Background(), &proto.Empty{})
	if err != nil {
		return err
	}
	node.OutClients[port] = client
	node.Replies[port] = make(chan bool, 1)
	return nil
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
