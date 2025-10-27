package internal

import (
	"crypto/sha256"
	"errors"
	"hash"

	"github.com/epicseven-cup/envgo"
)

type MarkNode struct {
	hash []byte
}

func NewMarkNode(data []byte) (*MarkNode, error) {
	size, err := envgo.GetValueOrDefault("MARK_SIZE", 256)
	if err != nil {
		return nil, err
	}

	if size < len(data) {
		return nil, errors.New("data size is larger than configure node size")
	}

	hashType, err := envgo.GetValueOrDefault("HASH_TYPE", "SHA256")
	if err != nil {
		return nil, err
	}

	var h hash.Hash

	switch hashType {
	case "SHA256":
		h = sha256.New()
	default:
		h = sha256.New()
	}

	// Writes data
	h.Write(data)

	return &MarkNode{
		hash: h.Sum(nil),
	}, nil
}
