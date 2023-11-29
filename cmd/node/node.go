package main

import (
	"PessiTorrent/internal/cli"
	"PessiTorrent/internal/logger"
	"PessiTorrent/internal/protocol"
	"PessiTorrent/internal/structures"
	"PessiTorrent/internal/transport"
	"PessiTorrent/internal/utils"
	"net"
)

type Node struct {
	trackerAddr string
	udpPort     uint16
	connected   bool // Whether the node is connected to the tracker or not

	conn transport.TCPConnection
	srv  transport.UDPServer

	published   structures.SynchronizedMap[*File]
	pending     structures.SynchronizedMap[*File]
	forDownload structures.SynchronizedMap[*ForDownloadFile]

	quitChannel chan struct{}
}

func NewNode(trackerAddr string, udpPort uint16) Node {
	return Node{
		trackerAddr: trackerAddr,
		udpPort:     udpPort,

		pending:     structures.NewSynchronizedMap[*File](),
		published:   structures.NewSynchronizedMap[*File](),
		forDownload: structures.NewSynchronizedMap[*ForDownloadFile](),

		quitChannel: make(chan struct{}),
	}
}

func (n *Node) Start() {
	go n.startTCP()
	go n.startUDP()
	go n.startCLI()
	go n.notifyTracker()

	<-n.quitChannel
}

func (n *Node) startTCP() {
	conn, err := net.Dial("tcp4", n.trackerAddr)
	if err != nil {
		logger.Error("Failed to connect to tracker: %s", err)
		return
	}

	n.connected = true
	n.conn = transport.NewTCPConnection(conn, n.HandlePackets, n.Stop)
	go n.conn.Start()

	logger.Info("Connected to tracker on %s", n.trackerAddr)
}

func (n *Node) startUDP() {
	udpAddr := net.UDPAddr{
		IP:   net.IPv4zero,
		Port: int(n.udpPort),
	}

	conn, err := net.ListenUDP("udp4", &udpAddr)
	if err != nil {
		logger.Error("Failed to start UDP server: %s", err)
		return
	}

	n.srv = transport.NewUDPServer(*conn, n.HandleUDPPackets, func() {})
	go n.srv.Start()

	logger.Info("UDP server started on %s", udpAddr.String())
}

func (n *Node) startCLI() {
	console := cli.NewConsole()
	defer console.Close()
	logger.SetLogger(&console)

	c := cli.NewCLI(n.Stop, console)
	c.AddCommand("publish", "<file name>", "", 1, n.publish)
	c.AddCommand("request", "<file name>", "", 1, n.requestFile)
	c.AddCommand("status", "", "Show the status of the node", 0, n.status)
	c.AddCommand("remove", "<file name>", "", 1, n.removeFile)
	c.Start()
}

func (n *Node) notifyTracker() {
	if !n.connected {
		return
	}

	ipAddr := utils.TCPAddrToBytes(n.conn.LocalAddr())
	packet := protocol.NewInitPacket(ipAddr, n.udpPort)
	n.conn.EnqueuePacket(&packet)
}

func (n *Node) Stop() {
	n.srv.Stop()
	n.quitChannel <- struct{}{}
	close(n.quitChannel)
}