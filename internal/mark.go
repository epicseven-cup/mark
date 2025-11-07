package internal

import (
	"crypto/sha256"
	"errors"
	"hash"
	"math"

	"github.com/epicseven-cup/envgo"
)

type MarkNode struct {
	hash  []byte
	level uint64
	tag   uint64
}

func NewMarkNode(level uint64, tag uint64, data []byte) (*MarkNode, error) {
	size, err := envgo.GetValueOrDefault("MARK_SIZE", DEFAULT_CHUNK_SIZE)
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
	h := detectHash(hashType)

	// Writes data
	h.Write(data)

	return &MarkNode{
		hash:  h.Sum(nil),
		level: level,
		tag:   tag,
	}, nil
}

func detectHash(hType string) hash.Hash {

	var h hash.Hash

	switch hType {
	case "SHA256":
		h = sha256.New()
	default:
		h = sha256.New()
	}
	return h

}

func computeNewNode(m *MarkNode, n *MarkNode) (*MarkNode, error) {

	hashType, err := envgo.GetValueOrDefault("HASH_TYPE", "SHA256")
	if err != nil {
		return nil, err
	}

	h := detectHash(hashType)
	h.Write(m.hash)
	h.Write(n.hash)
	return &MarkNode{
		hash:  h.Sum(nil),
		level: m.level + 1,
		tag:   uint64(math.Ceil(float64(m.tag / 2))),
	}, nil
}

// Writes in order for hashing, but uses the hash type of the bigger tag
func (n *MarkNode) Hash(m *MarkNode) (*MarkNode, error) {
	if m.level != n.level {
		return nil, errors.New("they must be at the same level")
	}
	if m.tag < n.tag {
		return computeNewNode(m, n)
	} else {
		return computeNewNode(n, m)
	}
}
