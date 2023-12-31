package utils

import (
	"crypto/sha1"
	"fmt"
	"io"
	"math"
	"os"
)

func HashFile(file *os.File) ([20]byte, error) {
	h := sha1.New()
	if _, err := io.Copy(h, file); err != nil {
		return [20]byte{}, fmt.Errorf("error copying file: %v", err)
	}

	hash := h.Sum(nil)
	var hashArr [20]byte
	copy(hashArr[:], hash[:20])

	return hashArr, nil
}

func HashFileChunks(file *os.File, dest *[][20]byte) (uint64, error) {
	_, err := file.Seek(0, 0)
	if err != nil {
		return 0, fmt.Errorf("error seeking file: %v", err)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		return 0, fmt.Errorf("error reading file content: %v", err)
	}

	_, err = file.Seek(0, 0)
	if err != nil {
		return 0, fmt.Errorf("error seeking file: %v", err)
	}

	fileSize := uint64(len(content))
	if fileSize == 0 {
		return 0, fmt.Errorf("file is empty")
	}

	chunkSize := ChunkSize(fileSize)
	numChunks := uint64(math.Ceil(float64(fileSize) / float64(chunkSize)))

	chunkHashes := make([][20]byte, numChunks)
	for i := uint64(0); i < numChunks; i++ {
		if i == numChunks-1 {
			chunkHashes[i] = sha1.Sum(content[i*chunkSize:])
		} else {
			chunkHashes[i] = sha1.Sum(content[i*chunkSize : (i+1)*chunkSize])
		}
	}

	*dest = chunkHashes

	return fileSize, nil
}

func HashChunk(chunk []byte) [20]byte {
	return sha1.Sum(chunk)
}

// FileSize -> bytes
// ChunkSize -> bytes
func ChunkSize(fileSize uint64) uint64 {
	const chunkBlockSize = 16000         // bytes
	const chunkCountMultiplier = 1 << 16 // 2^16

	// Calculate the chunk size using the provided equation
	return uint64(math.Ceil(float64(fileSize)/(float64(chunkCountMultiplier)*float64(chunkBlockSize)))) * chunkBlockSize
}
