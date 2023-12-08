package main

import (
	"PessiTorrent/internal/filewriter"
	"PessiTorrent/internal/protocol"
	"PessiTorrent/internal/structures"
	"net"
	"time"
)

const (
	ChunkTimeout = 3 * time.Second
)

type File struct {
	FileName string
	Path     string
}

func NewFile(fileName string, path string) File {
	return File{
		FileName: fileName,
		Path:     path,
	}
}

type ForDownloadFile struct {
	FileName   string
	FileHash   [20]byte
	FileSize   uint64
	FileWriter *filewriter.FileWriter

	NumberOfChunks uint16
	Chunks         structures.SynchronizedList[ChunkInfo]

	Nodes structures.SynchronizedMap[*net.UDPAddr, *NodeInfo]
}

type ChunkInfo struct {
	Index      uint16
	Downloaded bool
	Hash       [20]byte
}

type NodeInfo struct {
	// Chunk index -> Last time chunk was requested
	Chunks structures.SynchronizedMap[uint16, time.Time]
}

func NewForDownloadFile(fileName string) *ForDownloadFile {
	return &ForDownloadFile{
		FileName: fileName,
	}
}

func (f *ForDownloadFile) SetData(fileHash [20]byte, chunkHashes [][20]byte, fileSize uint64, numberOfChunks uint16) error {
	f.FileHash = fileHash
	f.FileSize = fileSize
	fileWriter, err := filewriter.NewFileWriter(f.FileName, fileSize)
	if err != nil {
		return err
	}
	f.FileWriter = fileWriter
	go f.FileWriter.Start()

	f.NumberOfChunks = numberOfChunks
	f.Chunks = structures.NewSynchronizedListWithInitialSize[ChunkInfo](uint(numberOfChunks))
	for i := 0; i < int(numberOfChunks); i++ {
		_ = f.Chunks.Set(uint(i), ChunkInfo{
			Index:      uint16(i),
			Downloaded: false,
			Hash:       chunkHashes[i],
		})
	}

	f.Nodes = structures.NewSynchronizedMap[*net.UDPAddr, *NodeInfo]()

	return nil
}

func (f *ForDownloadFile) IsFileDownloaded() bool {
	return f.LengthOfMissingChunks() == 0
}

func (f *ForDownloadFile) AddNode(nodeAddr *net.UDPAddr, bitfield []uint8) {
	nodeInfo := NodeInfo{
		Chunks: structures.NewSynchronizedMap[uint16, time.Time](),
	}

	decoded := protocol.DecodeBitField(bitfield)
	for _, chunkIndex := range decoded {
		nodeInfo.Chunks.Put(chunkIndex, time.Time{})
	}

	f.Nodes.Put(nodeAddr, &nodeInfo)
}

func (f *ForDownloadFile) MarkChunkAsRequested(chunkIndex uint16, nodeInfo *NodeInfo) {
	nodeInfo.Chunks.Put(chunkIndex, time.Now())
}

func (f *ForDownloadFile) MarkChunkAsDownloaded(chunkIndex uint16) {
	chunk, _ := f.Chunks.Get(uint(chunkIndex))
	chunk.Downloaded = true
	_ = f.Chunks.Set(uint(chunkIndex), chunk)
}

func (f *ForDownloadFile) ChunkAlreadyDownloaded(chunkIndex uint16) bool {
	chunk, _ := f.Chunks.Get(uint(chunkIndex))
	return chunk.Downloaded
}

func (f *ForDownloadFile) GetChunkHash(chunkIndex uint16) [20]byte {
	chunk, _ := f.Chunks.Get(uint(chunkIndex))
	return chunk.Hash
}

func (f *ForDownloadFile) GetMissingChunks() []uint {
	missingChunks := f.Chunks.IndexesWhere(func(chunk ChunkInfo) bool {
		return !chunk.Downloaded
	})

	return missingChunks
}

func (f *ForDownloadFile) LengthOfMissingChunks() int {
	return len(f.GetMissingChunks())
}

func (n *NodeInfo) ShouldRequestChunk(chunkIndex uint16) bool {
	chunk, ok := n.Chunks.Get(chunkIndex)
	if !ok {
		return false
	}

	// Chunk was not requested yet or it was requested more than 3 seconds ago
	return chunk == time.Time{} || time.Since(chunk) > ChunkTimeout
}

func (f *ForDownloadFile) SaveChunkToDisk(chunkIndex uint16, chunkContent []uint8) {
	f.FileWriter.EnqueueChunkToWrite(chunkIndex, chunkContent)
}
