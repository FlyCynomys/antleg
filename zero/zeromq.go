package zero

import (
	zmq "github.com/pebbe/zmq4"
)

func Init() {
	client, _ := zmq.NewSocket(zmq.ROUTER)
	client.Close()
}

const (
	NodeTypeRouter = iota << 1
)

type FuncNode struct {
	NodeType uint
}

type TransferNode struct {
}
