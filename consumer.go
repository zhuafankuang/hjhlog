package main

import (
	. "logs.7gz.com/apis"
	"sync"
)

func consumer(wg *sync.WaitGroup) {
	Coonsumer(wg)
}
