package internal

import (
	"log"
	"math"
	"os"
	"runtime"
	"syscall"

	"github.com/epicseven-cup/envgo"
)

const DEFAULT_CHUNK_SIZE = 256000000

var logger *log.Logger
var chunkSize int
var workerSize int
var layers []chan *MarkNode

type JobPacket struct {
	start uint64
	end   uint64
}

func init() {
	//logger = log.Default()
	//logger.SetFlags(log.LstdFlags | log.Lshortfile)
	var err error
	chunkSize, err = envgo.GetValueOrDefault("MARK_SIZE", DEFAULT_CHUNK_SIZE)
	if err != nil {
		log.Fatal(err)
	}
	workerSize, err = envgo.GetValueOrDefault("MARK_WORKER", runtime.NumCPU())
	if err != nil {
		logger.Fatal(err)
	}

	workerSize -= 1
	//
	//logger.Println("MARK_SIZE =", chunkSize)
	//logger.Println("MARK_WORKER =", workerSize)

}

func MerakHash(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileStats, err := file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fileStats.Size()

	// Check the math in a bit
	totalChunk := int64(math.Ceil(float64(fileSize) / float64(chunkSize)))
	expectedHeight := uint64(math.Log2(float64(totalChunk))) + 1
	//
	//logger.Println("FileSize:", fileSize)
	//logger.Println("TotalChunk:", totalChunk)
	//logger.Println("WorkerPerChunk:", workerPerChunk)
	//logger.Println("ExpectedHeight:", expectedHeight)

	layers = make([]chan *MarkNode, expectedHeight)
	for i := range layers {
		layers[i] = make(chan *MarkNode, 256)
	}

	for i := range len(layers) - 1 {
		go pipeLineBuildTree(uint64(i))
	}

	mmap, err := syscall.Mmap(int(file.Fd()), 0, int(fileSize), syscall.PROT_READ, syscall.MAP_SHARED)
	jobs := make([]chan JobPacket, workerSize)
	for i := 0; i < workerSize; i++ {
		jobs[i] = make(chan JobPacket)
		go worker(mmap, jobs[i])
	}
	currentIndex := 0
	currentWorker := 0
	for currentIndex < int(fileSize) {
		end := uint64(math.Min(float64(currentIndex+chunkSize), float64(fileSize)))
		//logger.Println("CurrentWorker:", currentWorker)
		//logger.Println("CurrentIndex:", currentIndex)
		//logger.Println("End:", end)
		packet := JobPacket{
			start: uint64(currentIndex),
			end:   end,
		}
		jobs[currentWorker] <- packet
		currentIndex += chunkSize
		//logger.Println("After Current Index:", currentIndex)
		currentWorker = (currentWorker + 1) % workerSize
	}

	if err != nil {
		return nil, err
	}

	h := <-layers[expectedHeight-1]
	return h.hash, nil

}

func worker(mmp []byte, channel chan JobPacket) {
	for p := range channel {
		buffer := mmp[p.start:p.end]
		//logger.Println("Buffer", buffer)
		//logger.Println("Start", current)
		//logger.Println("End", current)
		tag := p.start / uint64(chunkSize)
		//logger.Println("Tag", tag)
		n, err := NewMarkNode(0, tag, buffer)
		if err != nil {
			logger.Fatal(err)
		}
		layers[0] <- n
	}

}

func pipeLineBuildTree(level uint64) {

	cache := make(map[uint64]*MarkNode)

	for n := range layers[level] {
		//logger.Println("Level:", level, n)
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

			if level+1 > uint64(len(layers)-1) {
				logger.Fatal("Reached max number of layers", n.tag)
				return
			}

			layers[level+1] <- newN
		} else {
			cache[n.tag] = n
		}
	}
}
