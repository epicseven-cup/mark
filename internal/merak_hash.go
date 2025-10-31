package internal

import (
	"log"
	"math"
	"os"
	"sync"

	"github.com/epicseven-cup/envgo"
)

type FileJob struct {
	chunkSize  int64
	startIndex int64
}

func MerakHash(path string) string {
	logger := log.Default()
	ws, err := envgo.GetValueOrDefault("MARK_WORKER", 5)
	if err != nil {
		panic(err)
	}

	cs, err := envgo.GetValueOrDefault("MARK_SIZE", 256)
	if err != nil {
		panic(err)
	}

	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}

	defer file.Close()

	fs, err := file.Stat()
	if err != nil {
		panic(err)
	}

	totalChunks := fs.Size() / int64(cs)
	expectedHeight := int(math.Log2(float64(totalChunks))) + 1

	wg := new(sync.WaitGroup)

	job := make(chan FileJob)
	jobWg := new(sync.WaitGroup)

	pool := sync.Pool{
		New: func() interface{} {
			return make([]byte, cs)
		},
	}

	layers := make([]chan *MarkNode, expectedHeight)

	for i := 0; i < expectedHeight; i++ {
		layers[i] = make(chan *MarkNode, int(totalChunks)*100)
	}

	for i := 0; i < expectedHeight-1; i++ {
		wg.Add(1)
		maxNodeLevel := math.Round(float64(int(totalChunks) / int(math.Exp2(float64(i)))))
		go func() {
			defer wg.Done()
			cache := make(map[int]*MarkNode)

			for n := range layers[i] {
				var partner *MarkNode
				var ok bool
				logger.Println("Tag", n.tag)
				logger.Println("Level", n.level)
				if n.tag%2 == 0 {
					// the tag is even therefor look forward to check
					partner, ok = cache[n.tag+1]
				} else {
					partner, ok = cache[n.tag-1]
				}

				if ok {
					logger.Println("Partner", partner.tag)
					logger.Println("N", n.tag)
					logger.Println("level", n.level)
					newN, err := partner.Hash(n)
					if err != nil {
						panic(err)
					}

					if i+1 > len(layers)-1 {
						panic("going above the max layer")
					}
					layers[i+1] <- newN
				}
				cache[n.tag] = n

				logger.Println("MaxNodeLevel", maxNodeLevel)

				if int(maxNodeLevel) == len(cache) {
					logger.Println("+========+")
					logger.Println("MaxLevel Node", maxNodeLevel)
					logger.Println("Cache", len(cache))
					logger.Println("+========+")
					close(layers[i])
				}

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

	index := int64(0)
	for ; index < fs.Size(); index = index + int64(cs) {
		// Creating the filejobs for the job channel trigger
		chunkSize := min(int64(cs), fs.Size()-index)
		job <- FileJob{
			chunkSize:  chunkSize,
			startIndex: index,
		}
	}
	if (index/int64(cs))%2 == 0 {
		logger.Println("Padding", index/int64(cs))
		padding, err := NewMarkNode(0, index/int64(cs), []byte{})
		if err != nil {
			panic(err)
		}
		layers[0] <- padding
	}

	close(job)
	jobWg.Wait()
	wg.Wait()
	nn := <-layers[expectedHeight-1]
	log.Println("Hello world")
	log.Println("Final", nn.tag)
	log.Println("Final", nn.level)
	//for c := 0; c < expectedHeight; c++ {
	//	close(layers[expectedHeight-c-1])
	//}

	return string(nn.hash)
}

func worker(file *os.File, s *sync.Pool, startIndex int64) (*MarkNode, error) {
	buffer := s.Get().([]byte)
	defer s.Put(buffer)
	_, err := file.ReadAt(buffer, startIndex)
	if err != nil {
		return nil, err
	}

	cs, err := envgo.GetValueOrDefault("MARK_SIZE", 256)
	if err != nil {
		panic(err)
	}

	mn, err := NewMarkNode(0, startIndex/int64(cs), buffer)
	if err != nil {
		return nil, err
	}

	return mn, nil
}
