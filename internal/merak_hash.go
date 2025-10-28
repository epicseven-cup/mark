package internal

import (
	"fmt"
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

	job := make(chan FileJob)
	wg := new(sync.WaitGroup)
	jobWg := new(sync.WaitGroup)

	pool := sync.Pool{
		New: func() interface{} {
			return make([]byte, cs)
		},
	}

	nodes := make(chan *MarkNode, 128)
	ans := make(chan *MarkNode)
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(nodes)
		cache := make([]map[int]*MarkNode, 128)

		for i := range cache {
			cache[i] = make(map[int]*MarkNode)
		}
		endpoint := -1
		for n := range nodes {
			if n == nil {
				continue
			}

			logger.Println("merak hash tag", n.tag)
			logger.Println("merak hash level", n.level)
			logger.Println("merak hash cache", cache)
			if n.level == -1 {
				endpoint = int(math.Ceil(math.Log2(float64(n.tag)))) - 1
				log.Println("endpoint", endpoint)
				continue
			}

			// incase the layers are too big like wtf bro
			for len(cache) < n.level {
				cache = append(cache, make(map[int]*MarkNode))
			}

			var partner *MarkNode
			var ok bool
			if n.tag%2 == 0 {
				// the tag is even therefor look forward to check
				partner, ok = cache[n.level][n.tag+1]
			} else {
				partner, ok = cache[n.level][n.tag-1]
			}

			if ok {
				logger.Println("Partner", partner.tag)
				logger.Println("N", n.tag)
				newN, err := partner.Hash(n)
				if err != nil {
					panic(err)
				}
				nodes <- newN
			} else {
				cache[n.level][n.tag] = n
			}

			if endpoint > -1 {
				v, ok := cache[endpoint][0]
				if ok && v != nil {
					fmt.Println(v)
					ans <- v
					return
				}

			}

		}
	}()

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
				nodes <- n
			}
		}()
	}

	fs, err := file.Stat()
	if err != nil {
		panic(err)
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
		nodes <- padding
	}

	// sends the final packet as meta data
	meta, err := NewMarkNode(-1, index/int64(cs), nil)
	if err != nil {
		panic(err)
	}
	nodes <- meta

	close(job)
	jobWg.Wait()
	nn := <-ans
	close(ans)
	wg.Wait()

	fmt.Println(nn)

	return string(nn.level)
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
