package utils

import (
  "crypto/sha1"
  "io"
	"math"
	"os"
  "fmt"
)

func HashFile(file *os.File) ([20]byte, error) {
  _, err := file.Seek(0, 0)
	content, err := io.ReadAll(file)
	if err != nil {
		return [20]byte{}, fmt.Errorf("error reading file content: %v", err)
	}
	_, err = file.Seek(0, 0)

  return sha1.Sum(content), nil
}

func HashFileChunks(file *os.File, dest *[][20]byte) (uint64, error) {
  _, err := file.Seek(0, 0)
	content, err := io.ReadAll(file)
	if err != nil {
		return 0, fmt.Errorf("error reading file content: %v", err)
	}
	_, err = file.Seek(0, 0)

  // Calculate the chunk size
  chunkSize := ChunkSize(uint64(len(content)))
  numChunks := uint64(math.Ceil(float64(len(content)) / float64(chunkSize)))

  chunkHashes := make([][20]byte, numChunks)
  for i := uint64(0); i < numChunks; i++ {
    if i == numChunks - 1 {
      chunkHashes[i] = sha1.Sum(content[i * chunkSize:])
    } else {
      chunkHashes[i] = sha1.Sum(content[i * chunkSize:(i + 1) * chunkSize])
    }
  }

  *dest = chunkHashes
  return chunkSize, nil
}

// FileSize -> bytes
// ChunkSize -> bytes
func ChunkSize(fileSize uint64) uint64 {
	const chunkBlockSize = 16000 // bytes
	const chunkCountMultiplier = 1 << 16 // 2^16

	// Calculate the chunk size using the provided equation
	return uint64(math.Ceil(float64(fileSize) / (float64(chunkCountMultiplier) * float64(chunkBlockSize)))) * chunkBlockSize
}