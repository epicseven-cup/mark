package internal

import (
	"encoding/hex"
	"log"
	"math"
	"os"
	"sync"

	"github.com/epicseven-cup/envgo"
)

type FileJob struct {
	startIndex int
}

func MerakHash(path string) string {
	logger := log.Default()
	ws, err := envgo.GetValueOrDefault("MARK_WORKER", 5)
	if err != nil {
		logger.Fatal(err)
		return ""
	}

	cs, err := envgo.GetValueOrDefault("MARK_SIZE", 256)
	if err != nil {
		logger.Fatal(err)
		return ""
	}

	file, err := os.Open(path)
	if err != nil {
		logger.Fatal(err)
		return ""
	}

	defer file.Close()

	fileStat, err := file.Stat()
	if err != nil {
		logger.Fatal(err)
		return ""
	}

	fileSize := int(fileStat.Size())

	totalChunks := fileSize / cs

	// Padding the chunks
	if totalChunks%2 != 0 {
		totalChunks++
	}
	expectedHeight := uint64(math.Log2(float64(totalChunks))) + 1

	job := make(chan FileJob)
	jobWg := new(sync.WaitGroup)

	pool := sync.Pool{
		New: func() interface{} {
			return make([]byte, cs)
		},
	}

	layers := make([]chan *MarkNode, expectedHeight)

	layers[expectedHeight-1] = make(chan *MarkNode, 1)

	for i := uint64(0); i < expectedHeight-1; i++ {

		maxNodeLevel := int(math.Round(float64(totalChunks / int(math.Exp2(float64(i))))))
		// Buffer to the max node per level
		layers[i] = make(chan *MarkNode, maxNodeLevel)

		go func() {
			cache := make(map[int]*MarkNode)

			for n := range layers[i] {
				var partner *MarkNode
				var ok bool
				if n.tag%2 == 0 {
					// the tag is even therefor look forward to check
					partner, ok = cache[n.tag+1]
				} else {
					partner, ok = cache[n.tag-1]
				}

				if ok {
					newN, err := partner.Hash(n)
					if err != nil {
						logger.Fatal(err)
						return
					}

					if i+1 > uint64(len(layers)-1) {
						logger.Fatal("Reached max number of layers", n.tag)
						return
					}

					layers[i+1] <- newN
				}
				cache[n.tag] = n
			}
		}()
	}

	for i := 0; i < ws; i++ {
		jobWg.Add(1)
		// Working spliting the files into chunks
		go func() {
			defer jobWg.Done()
			for fileJob := range job {
				// TODO: the worker can be concurrent too
				n, err := worker(file, &pool, fileJob.startIndex)
				if err != nil {
					return
				}
				// Sending the created nodes into the node channel for grouping
				layers[0] <- n
			}
		}()
	}

	index := 0
	for ; index < fileSize; index = index + cs {
		// Creating the filejobs for the job channel trigger
		job <- FileJob{
			startIndex: index,
		}
	}
	if (index/cs)%2 == 0 {
		padding, err := NewMarkNode(0, index/cs, []byte{})
		if err != nil {
			panic(err)
		}
		layers[0] <- padding
	}

	close(job)
	jobWg.Wait()
	nn := <-layers[expectedHeight-1]
	return hex.EncodeToString(nn.hash)
}

func worker(file *os.File, s *sync.Pool, startIndex int) (*MarkNode, error) {
	buffer := s.Get().([]byte)
	defer s.Put(buffer)
	_, err := file.ReadAt(buffer, int64(startIndex))
	if err != nil {
		return nil, err
	}

	cs, err := envgo.GetValueOrDefault("MARK_SIZE", 256)
	if err != nil {
		panic(err)
	}

	mn, err := NewMarkNode(0, startIndex/cs, buffer)
	if err != nil {
		return nil, err
	}

	return mn, nil
}
