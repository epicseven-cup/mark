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
	flag.Parse()
	path := "test.iso"
	s, err := internal.MerakHash(path)
	if err != nil {
		panic(err)
	}
	fmt.Println(s)
}
