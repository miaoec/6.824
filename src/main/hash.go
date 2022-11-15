package main

import (
	"fmt"
	"hash/fnv"
	"os"
	"strconv"
)

func main() {
	s := os.Args[1]
	mod, _ := strconv.Atoi(os.Args[2])
	h := fnv.New32a()
	h.Write([]byte(s))
	fmt.Println(int(h.Sum32()&0x7fffffff) % mod)
}
