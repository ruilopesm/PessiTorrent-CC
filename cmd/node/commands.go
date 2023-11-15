package main

import (
	"PessiTorrent/internal/packets"
	"fmt"
	"os"
)

// request <file>
func (n *Node) requestFile(args []string) error {
	filename := args[0]

	var packet packets.RequestFilePacket
	packet.Create(filename)
	err := n.conn.WritePacket(packet)
	if err != nil {
		return err
	}

	// TODO: handle file not found packet
	fileHashesPacket, _, err := n.conn.ReadPacket()
	if err != nil {
		return err
	}

	pf := fileHashesPacket.(*packets.PublishFilePacket)
	f := File{
		filename:    filename,
		fileHash:    pf.FileHash,
		chunkHashes: pf.ChunkHashes,
	}
	n.files.Put(filename, &f)

	// Read response packets
	responsePacket, _, err := n.conn.ReadPacket()
	fmt.Printf("node %v, %v has the file\n", responsePacket.(*packets.AnswerNodesPacket).NodeIPAddr, responsePacket.(*packets.AnswerNodesPacket).UDPPort)
	if err != nil {
		return err
	}

	// Read response packets
	for {
		if responsePacket.(*packets.AnswerNodesPacket).SequenceNumber == 0 {
			break
		}

		responsePacket, _, err = n.conn.ReadPacket()
		fmt.Printf("node %v, %v has the file\n", responsePacket.(*packets.AnswerNodesPacket).NodeIPAddr, responsePacket.(*packets.AnswerNodesPacket).UDPPort)
		if err != nil {
			return err
		}
	}

	return nil
}

// publish <path>
func (n *Node) publish(args []string) error {
	filePath := args[0]

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}

	if fileInfo.IsDir() {
		files, err := file.Readdir(-1)
		if err != nil {
			return err
		}

		// Add trailing slash if absent
		if filePath[len(filePath)-1] != '/' {
			filePath = filePath + "/"
		}

		for _, f := range files {
			err := n.publish([]string{filePath + f.Name()})
			if err != nil {
				return err
			}
		}

	} else {
		fmt.Println("Adding file to internal memory:", filePath)
		f, err := n.AddFile(filePath)
		if err != nil {
			return err
		}

		fmt.Println("Sending file to tracker:", filePath)
		var packet packets.PublishFilePacket
		packet.Create(f.filename, f.fileHash, f.chunkHashes)
		err = n.conn.WritePacket(packet)
		if err != nil {
			return err
		}
	}

	return nil
}

// status
func (n *Node) status(args []string) error {
	n.files.ForEach(func(filename string, file *File) {
		fmt.Println("----------------------------------------")
		fmt.Printf("File: %v\n", file.filename)
		fmt.Printf("Filepath: %v\n", file.filepath)
		fmt.Printf("File hash: %v\n", file.fileHash)
		fmt.Printf("Chunk hashes: %v\n", file.chunkHashes)
		fmt.Printf("Bitfield: %b\n", file.bitfield)
	})

	return nil
}

func (n *Node) removeFile(args []string) error {
	filename := args[0]

	n.RemoveFile(filename)

	var packet packets.RemoveFilePacket
	packet.Create(filename)

	err := n.conn.WritePacket(packet)
	if err != nil {
		return err
	}

	return nil
}
