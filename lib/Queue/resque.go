package resque

import (
	kafka "logs.7gz.com/lib/Queue/kafka"
	"sync"
)

type Logs struct {
	Id string  `bson:"id"`
	Source string  `bson:"source"`
	Bu string  `bson:"bu"`
	Type int  `bson:"type"`
	Level int  `bson:"level"`
	//Status int  `bson:"status"`
	Contents string  `bson:"contents"`
	Addtime int64  `bson:"addtime"`
	Ip string  `bson:"ip"`
}

//入队列
func Producer(job Logs) (status int) {
	status = 1
	logs := kafka.Logs{Id:job.Id,Source:job.Source,Bu:job.Bu,Type:job.Type,Level:job.Level,Contents:job.Contents,Addtime:job.Addtime,Ip:job.Ip}
	//err := kafka.SaramaProducer(logs)
	err := kafka.SaramaProducerNew(logs)
	if err != nil {
		status = 0
	}
	return status
}

//出队列
func Consumer(w *sync.WaitGroup) (status int, msg string) {
	//status,msg = kafka.SaramaConsumer()
	status,msg = kafka.SaramaConsumerNew(w)
	//if status != 1 {
	//	log.Fatalln(msg)
	//}
	return status,msg
}

//出队列
func DoConsumer() (status int, msg string) {
	//status,msg = kafka.SaramaConsumer()
	status,msg = kafka.DoSaramaConsumerNew()
	//if status != 1 {
	//	log.Fatalln(msg)
	//}
	return status,msg
}
