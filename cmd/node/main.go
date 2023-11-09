package main

import (
	"PessiTorrent/internal/cli"
	"PessiTorrent/internal/connection"
	"PessiTorrent/internal/packets"
	"PessiTorrent/internal/utils"
	"log"
	"net"
	"os"
	"strconv"
)

type Node struct {
	ipAddr     [4]byte
	serverAddr string
	udpPort    uint16
	conn       connection.Connection
	quitch     chan struct{}
}

func NewNode(serverAddr string, listenUDPPort string) Node {
	// FIXME: This should be on another module
	udpPort, err := strconv.ParseUint(listenUDPPort, 10, 16)
	if err != nil {
		log.Fatal(err)
	}

	return Node{
		serverAddr: serverAddr,
		udpPort:    uint16(udpPort),
		quitch:     make(chan struct{}),
	}
}

func (n *Node) Start() error {
	conn, err := net.Dial("tcp4", n.serverAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	n.conn = connection.NewConnection(conn)
	n.ipAddr = utils.TCPAddrToBytes(conn.LocalAddr())

	// TODO: Listen on udp

	var packet packets.InitPacket
	packet.Create(n.ipAddr, n.udpPort)
	err = n.conn.WritePacket(packet)
	if err != nil {
		return err
	}

	cli := cli.NewCLI(n.stop)
	cli.AddCommand("request", "<file>", 1, n.requestFile)
	cli.AddCommand("publish", "<file>", 1, n.publishFile)
	go cli.Start()

	<-n.quitch

	return nil
}

func (n *Node) stop() {
	n.quitch <- struct{}{}
}

func main() {
	if len(os.Args) != 3 {
		log.Fatal("Usage: node <server address> <udp port>")
		return
	}

	// TODO: check if port is inside udp range

	node := NewNode(os.Args[1], os.Args[2])
	err := node.Start()
	if err != nil {
		log.Fatal(err)
	}
}
