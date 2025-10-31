package main

import (
	"flag"
	"fmt"

	"github.com/epicseven-cup/mark/internal"
)

var path string

func init() {
	flag.StringVar(&path, "path", "", "path to the file")
	flag.StringVar(&path, "p", "", "path to the file (shorthand)")
}

func main() {
	path = "image.png"
	flag.Parse()
	s := internal.MerakHash(path)
	fmt.Println(s)
}
