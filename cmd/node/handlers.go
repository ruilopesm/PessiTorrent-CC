package main

import (
	"PessiTorrent/internal/logger"
	"PessiTorrent/internal/protocol"
	"PessiTorrent/internal/transport"
	"PessiTorrent/internal/utils"
	"errors"
	"io"
	"net"
	"os"
	"time"
)

func (n *Node) HandlePackets(packet protocol.Packet, conn *transport.TCPConnection) {
	switch packet := packet.(type) {
	case *protocol.AnswerNodesPacket:
		n.handleAnswerNodesPacket(packet, conn)
	case *protocol.FileSuccessPacket:
		n.handleFileSuccessPacket(packet, conn)
	case *protocol.AlreadyExistsPacket:
		n.handleAlreadyExistsPacket(packet, conn)
	case *protocol.NotFoundPacket:
		n.handleNotFoundPacket(packet, conn)
	default:
		logger.Warn("Unknown packet type: %v.", packet)
	}
}

func (n *Node) HandleUDPPackets(packet protocol.Packet, addr *net.UDPAddr) {
	switch data := packet.(type) {
	case *protocol.ChunkPacket:
		n.handleChunkPacket(data, addr)
	case *protocol.RequestChunksPacket:
		n.handleRequestChunksPacket(data, addr)
	default:
		logger.Warn("Unknown packet type: %v.", data)
	}
}

// Handler for when a node requests, to the tracker, the list of nodes who have a specific file
func (n *Node) handleAnswerNodesPacket(packet *protocol.AnswerNodesPacket, conn *transport.TCPConnection) {
	// Update file in forDownload data structure
	forDownloadFile, ok := n.forDownload.Get(packet.FileName)
	if !ok {
		return // File was removed from forDownload files
	}

	logger.Info("Updating nodes who have chunks for file %s", packet.FileName)

	err := forDownloadFile.SetData(packet.FileHash, packet.ChunkHashes, packet.FileSize, uint16(len(packet.ChunkHashes)))
	if err != nil {
		logger.Error("Error setting data for file %s: %v", packet.FileName, err)
		return
	}

	for _, node := range packet.Nodes {
		udpAddr := net.UDPAddr{
			IP:   node.IPAddr[:],
			Port: int(node.Port),
		}

		ipAddr := utils.TCPAddrToBytes(n.conn.LocalAddr())

		if n.udpPort != node.Port || ipAddr != node.IPAddr { // Do not add itself to the list of nodes
			forDownloadFile.AddNode(&udpAddr, node.Bitfield)
		}
	}

	logger.Info("File %s information internally updated. Run 'status' in order to check for downloading files", packet.FileName)
}

// Handler for when a node publishes/removes a file in/from the network
func (n *Node) handleFileSuccessPacket(packet *protocol.FileSuccessPacket, conn *transport.TCPConnection) {
	switch packet.Type {
	case protocol.PublishFileType:
		logger.Info("File %s published in the network successfully", packet.FileName)

		// Remove file from pending and add it to published, since tracker has accepted it
		file, _ := n.pending.Get(packet.FileName)
		n.published.Put(packet.FileName, file)
		n.pending.Delete(packet.FileName)
	case protocol.RemoveFileType:
		logger.Info("File %s removed from the network successfully", packet.FileName)

		// Remove file from published, since tracker has removed it from the network
		n.published.Delete(packet.FileName)
	default:
		logger.Warn("Unknown file success packet type: %v", packet.Type)
	}
}

// Handler for when the file, the node is trying to publish, already exists in the network
func (n *Node) handleAlreadyExistsPacket(packet *protocol.AlreadyExistsPacket, conn *transport.TCPConnection) {
	logger.Info("File %s already exists in the network", packet.Filename)

	// Remove file from pending, since tracker has rejected it
	n.pending.Delete(packet.Filename)
}

// Handler for when the file, the node is trying to download, does not exist in the network
func (n *Node) handleNotFoundPacket(packet *protocol.NotFoundPacket, conn *transport.TCPConnection) {
	logger.Info("File %s was not found in the network", packet.Filename)

	// Remove file from downloading, since it does not exist
	n.forDownload.Delete(packet.Filename)
}

func (n *Node) handleChunkPacket(packet *protocol.ChunkPacket, addr *net.UDPAddr) {
	// Check if hash of chunk is correct
	forDownloadFile, ok := n.forDownload.Get(packet.FileName)
	if !ok {
		logger.Warn("File %s not found in forDownload files", packet.FileName)
		return
	}

	// Discard packet if chunk is already downloaded
	if forDownloadFile.ChunkAlreadyDownloaded(packet.Chunk) {
		return
	}

	// Discard packet if hash of chunk is not correct
	if forDownloadFile.GetChunkHash(packet.Chunk) != utils.HashChunk(packet.ChunkContent) {
		logger.Warn("Received incorrect hash of chunk %d of file %s", packet.Chunk, packet.FileName)
		return
	}

	nodeInfo, ok := forDownloadFile.Nodes.Get(addr.String())
	if !ok {
		logger.Warn("Node %s sent unrequested chunk from file %s", addr, packet.FileName)
	} else {
		requested, b := nodeInfo.GetLastTimeChunkWasRequested(packet.Chunk)
		if b && requested != (time.Time{}) {
			n.nodeStatistics.addDownloadedChunk(addr.String(), uint64(len(packet.ChunkContent)), requested, time.Now())
		}
	}

	// Write chunk to file
	forDownloadFile.WriteChunkToDisk(packet.Chunk, packet.ChunkContent)
}

func (n *Node) handleRequestChunksPacket(packet *protocol.RequestChunksPacket, addr *net.UDPAddr) {
	logger.Info("Request chunks packet received from %s", addr)

	// Get file from published files
	publishedFile, ok := n.published.Get(packet.FileName)
	if !ok {
		logger.Warn("File %s not found in published files", packet.FileName)
		// TODO: Find file in downloaded files or being downloaded files
		return
	}

	// Open file by the given path
	file, err := os.Open(publishedFile.Path)
	if err != nil {
		logger.Warn("Error opening file: %v", err)
		return
	}

	stats, _ := file.Stat()
	chunkSize := utils.ChunkSize(uint64(stats.Size()))

	// Send requested chunks
	for _, chunk := range packet.Chunks {
		logger.Info("Sending chunk %d of file %s to %s", chunk, packet.FileName, addr)

		// Seek to the beginning of the chunk
		_, err = file.Seek(int64(uint64(chunk)*chunkSize), 0)
		if err != nil {
			logger.Warn("Error seeking file: %v", err)
			return
		}

		// Read chunk bytes
		chunkContent := make([]byte, chunkSize)
		read, err := file.Read(chunkContent)
		if err != nil && !errors.Is(err, io.EOF) {
			logger.Warn("Error reading file: %v", err)
			return
		}

		// Send chunk bytes
		packet := protocol.NewChunkPacket(packet.FileName, chunk, chunkContent[:read])
		n.srv.SendPacket(&packet, addr)
		n.nodeStatistics.addUploadedBytes(chunkSize)
	}
}
