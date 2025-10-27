package internal

import (
	"os"
	"sync"

	"github.com/epicseven-cup/envgo"
)

type MarkNodeCollection struct {
	nodes []*MarkNode
	mutex sync.Mutex
}

type FileJob struct {
	chunkSize  int64
	startIndex int64
}

func (m *MarkNodeCollection) Append(node *MarkNode) int {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	m.nodes = append(m.nodes, node)
	return len(m.nodes)
}

func MerakHash(path string) string {
	ws, err := envgo.GetValueOrDefault("MARK_WORKER", 5)
	if err != nil {
		panic(err)
	}

	cs, err := envgo.GetValueOrDefault("MARK_SIZE", 256)
	if err != nil {
		panic(err)
	}
	mnc := MarkNodeCollection{
		nodes: make([]*MarkNode, ws),
	}

	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}

	job := make(chan FileJob)
	wg := new(sync.WaitGroup)

	pool := sync.Pool{
		New: func() interface{} {
			return make([]byte, cs)
		},
	}

	for i := 0; i < ws; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fileJob := range job {
				// TODO: the worker can be concurrent too
				err := worker(file, &mnc, &pool, fileJob.startIndex, fileJob.chunkSize)
				if err != nil {
					return
				}
			}
		}()
	}

	fs, err := file.Stat()
	if err != nil {
		panic(err)
	}

	for index := int64(0); index < fs.Size(); index = index + int64(cs) {
		chunkSize := min(int64(cs), fs.Size()-index)
		job <- FileJob{
			chunkSize:  chunkSize,
			startIndex: index,
		}
	}
	close(job)

	wg.Wait()

	return ""
}

func worker(file *os.File, nodeCollection *MarkNodeCollection, s *sync.Pool, startIndex int64, chunkSize int64) error {
	buffer := s.Get().([]byte)
	defer s.Put(buffer)
	_, err := file.ReadAt(buffer, startIndex)
	if err != nil {
		return err
	}

	mn, err := NewMarkNode(buffer)
	if err != nil {
		return err
	}

	nodeCollection.Append(mn)

	return nil
}
