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
	// OutClients are the clients this Node has to the other Nodes' servers.
	//Key corresponds to the port and id of the process the client connects to. This means lower port equals higher priority
	OutClients map[int64]proto.NodeClient
	Timestamp  chan int64 // Timestamp is a channel, which is meant to be 1 buffered, to prevent other threads from introducing race conditions
	PortId     int64      // PortId is both the port at which this Node's server is listening and the Node's id.
	State      State
	Replies    map[int64]chan bool // Replies maps PortId's to channels indicating a response. Should be 1 buffered.
	Queue      queue.Queue         // Queue is a simple FIFO queue of PortId's, the order of which this Node should reply to requests, when exiting CS
	Start      chan bool           // Start is a simple channel to indicate whether the channel may start attempting to enter the CS
}

// State represents the Node's relationship to the critical section.
type State int

const (
	// HELD indicates it is holding the critical section and by extension no other Node has the critical section
	HELD State = 1
	// RELEASED Node neither wants the critical section, nor does it currently hold it.
	RELEASED State = 2
	// WANTED indicates the channel wants to access the critical section, but does not currently hold it.
	WANTED State = 3
)

// Request is the logic that happens within Node, when it receives a Request rpc call. To be called within Enter method
func (node *Node) Request(ctx context.Context, msg *proto.LamportMessage) (*proto.Empty, error) {
	//timestamp at the receival of a request
	timestamp := node.RemoteEvent(msg.LamportTimestamp)
	utility.LogAsJson(utility.LogStruct{Timestamp: timestamp, Identifier: msg.GetProcessId(), Message: "Receive Request from [Identifier]"}, true)
	if node.State == HELD || (node.State == WANTED && node.HasHigherPriority(msg)) {
		node.Queue.Enqueue(msg.GetProcessId())
	} else {
		//timestamp when replying to the message i.e. on method return
		timestamp = node.LocalEvent()
		utility.LogAsJson(utility.LogStruct{Timestamp: timestamp, Identifier: msg.GetProcessId(), Message: "Send reply to [Identifier]"}, true)
		_, err := node.OutClients[msg.GetProcessId()].Reply(ctx, &proto.LamportMessage{LamportTimestamp: timestamp, ProcessId: node.PortId})
		if err != nil {
			utility.LogAsJson(utility.LogStruct{Timestamp: timestamp, Identifier: msg.GetProcessId(), Message: "Error when replying to [Identifier]: " + err.Error()}, false)
			log.Println("]")
			os.Exit(1)
			return nil, err
		}
	}
	return &proto.Empty{}, nil
}

// Reply is the logic that happens within the Node, when it receives a Reply rpc call.
// To be called within Request method or on Exit call, replying to every Node in its Queue
func (node *Node) Reply(ctx context.Context, msg *proto.LamportMessage) (*proto.Empty, error) {
	// Timestamp the event of receiving the reply from the Node that called Reply rpc to this Node
	timestamp := node.RemoteEvent(msg.LamportTimestamp)
	utility.LogAsJson(utility.LogStruct{Timestamp: timestamp, Identifier: msg.ProcessId, Message: "Received Reply from [Identifier]"}, true)
	// The Node now records whom it got a reply from
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
	msg := &proto.LamportMessage{LamportTimestamp: timestamp, ProcessId: node.PortId} //the message to be sent
	utility.LogAsJson(utility.LogStruct{Timestamp: timestamp, Identifier: node.PortId, Message: "Send Requests"}, true)
	for _, client := range node.OutClients {
		_, err := client.Request(context.Background(), msg)
		if err != nil {
			utility.LogAsJson(utility.LogStruct{Timestamp: timestamp, Identifier: node.PortId, Message: err.Error()}, true)
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

/*
Exit is called when the Node exits the critical section, replying to every Nodes' request in FIFO order.
To be called in CriticalSection method.
*/
func (node *Node) Exit() {
	node.State = RELEASED
	timestamp := node.LocalEvent()
	utility.LogAsJson(utility.LogStruct{Timestamp: timestamp, Identifier: node.PortId, Message: "Releasing critical section and replying to queue"}, true)
	msg := &proto.LamportMessage{LamportTimestamp: timestamp, ProcessId: node.PortId}
	for {
		if node.Queue.IsEmpty() {
			return
		}
		clientPort := node.Queue.Dequeue()
		utility.LogAsJson(utility.LogStruct{Timestamp: timestamp, Identifier: clientPort, Message: "Send Reply to [Identifier]"}, true)
		client := node.OutClients[clientPort]
		_, err := client.Request(context.Background(), msg)
		if err != nil {
			utility.LogAsJson(utility.LogStruct{Timestamp: timestamp, Identifier: node.PortId, Message: err.Error()}, true)
		}
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
	log.Println("[")
	node := CreateNodeServer(*port)
	go node.StartServer(node.PortId)
	//do internal logic
	go func() {
		err = node.InputHandler()
		if err != nil {
			utility.LogAsJson(utility.LogStruct{Timestamp: node.GetTimestamp(), Identifier: node.PortId, Message: err.Error()}, true)
		}
	}()

	go func() {
		<-node.Start
		for {
			if len(node.OutClients) >= 1 {
				time.Sleep(1 * time.Second)
				utility.LogAsJson(utility.LogStruct{Timestamp: node.GetTimestamp(), Identifier: node.PortId, Message: "Call Enter"}, true)
				node.Enter()
			}
		}
	}()

	select {}
}

// InputHandler is meant to be called as a go routine, which will handle all CLI inputs, such as connecting to other servers
func (node *Node) InputHandler() error {
	regex := regexp.MustCompile("(?:--connect|-c) *(?P<port>\\d{4})") //expect a four digit port number
	regex2 := regexp.MustCompile("--start")

	for {
		reader := bufio.NewReader(os.Stdin)
		msg, err := reader.ReadString('\n')
		if err != nil {
			utility.LogAsJson(utility.LogStruct{Timestamp: node.GetTimestamp(), Identifier: node.PortId, Message: err.Error()}, true)
			continue
		}
		match := regex.FindStringSubmatch(msg)
		match2 := regex2.FindStringSubmatch(msg)
		if match == nil && match2 == nil {
			utility.LogAsJson(utility.LogStruct{Timestamp: node.GetTimestamp(), Identifier: node.PortId, Message: "Invalid input: " + strings.Split(msg, "\n")[0]}, true)
			continue
		}

		if match2 != nil {
			node.Start <- true
			continue
		}

		port, _ := strconv.ParseInt(match[1], 10, 64)
		err = node.ConnectToNodeServer(port)
		if err != nil {
			utility.LogAsJson(utility.LogStruct{Timestamp: node.GetTimestamp(), Identifier: node.PortId, Message: err.Error()}, true)
			continue
		}
	}
}

func (node *Node) CriticalSection() {
	timestamp := node.LocalEvent()
	fmt.Printf("In critical section with client [%v] at timestamp %v\n", node.PortId, timestamp)
	utility.LogAsJson(utility.LogStruct{Timestamp: timestamp, Identifier: node.PortId, Message: "In Critical Section"}, true)
	node.Exit()
}

func (node *Node) StartServer(port int64) {
	grpcServer := grpc.NewServer()
	lis, err := net.Listen("tcp", ":"+strconv.FormatInt(port, 10))
	if err != nil {
		utility.LogAsJson(utility.LogStruct{Timestamp: 0, Identifier: node.PortId, Message: "Failed to listen. Error: " + err.Error()}, true)
		log.Println("]")
		os.Exit(1)
	}
	proto.RegisterNodeServer(grpcServer, node)
	err = grpcServer.Serve(lis)
	if err != nil {
		utility.LogAsJson(utility.LogStruct{Timestamp: 0, Identifier: node.PortId, Message: "Failed to serve. Error: " + err.Error()}, true)
		log.Println("]")
		os.Exit(1)
	}
}

func CreateNodeServer(id int64) Node {
	out := Node{
		PortId:     id,
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

// GetTimestamp returns the current timestamp without incrementing it
func (node *Node) GetTimestamp() int64 {
	timestamp := <-node.Timestamp
	node.Timestamp <- timestamp
	return timestamp
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

// HasHigherPriority checks whether the Node or the Node which sent the LamportMessage has priority according to
// the Ricart-Agrawala algorithm.
func (node *Node) HasHigherPriority(msg *proto.LamportMessage) bool {
	timestamp := node.GetTimestamp()
	if timestamp < msg.GetLamportTimestamp() || (timestamp == msg.GetLamportTimestamp()) && node.PortId < msg.ProcessId {
		return true
	}
	return false
}
