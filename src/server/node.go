package node

import (
	proto "Distributed-Systems-Assignment-04/src/grpc"
	"context"
	"log"
	"net"
	"strconv"

	"google.golang.org/grpc"
)

type NodeServer struct {
	proto.UnimplementedNodeServer
	OutClients map[string]proto.NodeClient //OutClients are the clients this Node has to the other Nodes' servers.
	Timestamp  chan int64
	Id         int64
	State      State
}

type State int

const (
	HELD     State = 1
	RELEASED State = 2
	WANTED   State = 3
)

func (server *NodeServer) Request(context.Context, *proto.LamportMessage) (*proto.Empty, error) {

	return nil, nil
}

func (server *NodeServer) Reply(context.Context, *proto.LamportMessage) (*proto.Empty, error) {
	return nil, nil
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
