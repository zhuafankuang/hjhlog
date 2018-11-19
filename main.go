package main

import (
	"github.com/fvbock/endless"
	"log"
	"os"
	"sync"
)

func main() {
	//获取路由
	router := initRouter()
	wg := &sync.WaitGroup{}
	wg.Add(2)
	//router.Run(":8880")
	go func() {
		err := endless.ListenAndServe(":8880", router)
		if err != nil {
			log.Println(err)
		}
		log.Println("Server on 8880 stopped")
		wg.Done()
	}()
	go consumer(wg)
	wg.Wait()
	log.Println("sync.waitGroup has crossed")
	os.Exit(0)
}
