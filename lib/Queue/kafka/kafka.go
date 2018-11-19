package kafka

import (
	"encoding/json"
	"fmt"
	"gopkg.in/mgo.v2/bson"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"github.com/JeffreyDing11223/kago"
	"github.com/Shopify/sarama"

	MongoDb "logs.7gz.com/database"
	conf "logs.7gz.com/lib/Func"
)

var (
	//导入配置文件
	kafkaMap  = conf.InitConfig("conf/kafka_conf.txt")
	mongoMap  = conf.InitConfig("conf/mongo_conf.txt")
	mongoinit = MongoDb.MongoInit()
	offsetfiledir = conf.GetKafkaOffsetDir("/offsetCfg")//设置kafka偏移量记录文件
)

var (
	topics = "log-topic"
)

var (
	// kafka 服务器地址,以及端口号,这里可以指定多个地址,使用逗号分隔开即可.
	//kafka_boker = "127.0.0.1:9092"
	wg sync.WaitGroup
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

//生产者
func SaramaProducer(job Logs) (err error) {

	config := sarama.NewConfig()
	//等待服务器所有副本都保存成功后的响应
	config.Producer.RequiredAcks = sarama.WaitForAll
	//随机向partition发送消息
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	//是否等待成功和失败后的响应,只有上面的RequireAcks设置不是NoReponse这里才有用.
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	//设置使用的kafka版本
	config.Version = sarama.V0_10_0_1

	//使用配置,新建一个异步生产者
	p, err := sarama.NewAsyncProducer([]string{kafkaMap["host"] + ":" + kafkaMap["port"]}, config)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer p.AsyncClose()

	//循环判断哪个通道发送过来数据.
	go func(p sarama.AsyncProducer) {
		errors := p.Errors()
		success := p.Successes()
		for {
			select {
			case err = <-errors:
				if err != nil {
					//glog.Errorln(err)
				}
			case <-success:
			}
		}
	}(p)

	for i := 0; i < 1; i++ {
		time.Sleep(500 * time.Millisecond)
		jsonString, errs := json.Marshal(job)
		if errs != nil {
			fmt.Println("Failed to get the json of logs: ", errs)
		}
		jobString := string(jsonString)
		// 发送的消息,主题。
		msg := &sarama.ProducerMessage{
			Topic: topics,
		}
		//将字符串转化为字节数组
		msg.Value = sarama.ByteEncoder(jobString)
		//使用通道发送
		p.Input() <- msg
	}
	return
}

//消费者
func SaramaConsumer() (status int, msg string) {
	status = 1
	msg = "Consumer success"
	if mongoinit != nil {
		status = 0
		msg = "Mongodb connect fail"
		return status, msg
	}
	config := sarama.NewConfig()
	//提交offset的间隔时间，每秒提交一次给kafka
	config.Consumer.Offsets.CommitInterval = 1 * time.Second
	//设置使用的kafka版本
	config.Version = sarama.V0_10_0_1
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	//新建consumer
	consumer, err := sarama.NewConsumer([]string{kafkaMap["host"] + ":" + kafkaMap["port"]}, config)
	if err != nil {
		status = 2
		msg = "Make consumer fail"
		return status, msg
	}
	//新建一个client，为了后面offsetManager做准备
	client, err := sarama.NewClient([]string{kafkaMap["host"] + ":" + kafkaMap["port"]}, config)
	if err != nil {
		status = 2
		msg = "client create error"
		return status, msg
	}
	defer client.Close()
	//新建offsetManager，为了能够手动控制offset
	offsetManager, err := sarama.NewOffsetManagerFromClient("group111", client)
	if err != nil {
		status = 2
		msg = "offsetManager create error"
		return status, msg
	}
	defer offsetManager.Close()
	//创建一个第2分区的offsetManager，每个partition都维护了自己的offset
	partitionOffsetManager, err := offsetManager.ManagePartition(topics, 2)
	if err != nil {
		status = 2
		msg = "partitionOffsetManager create error"
		return status, msg
	}
	defer partitionOffsetManager.Close()

	defer func() {
		if err = consumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	/*topics,_:=consumer.Topics()
	  fmt.Println(topics)
	  partitions,_:=consumer.Partitions(topics)
	  fmt.Println(partitions)*/
	//第一次的offset从kafka获取(发送OffsetFetchRequest)，之后从本地获取，由MarkOffset()得来
	nextOffset, _ := partitionOffsetManager.NextOffset()
	//创建一个分区consumer，从上次提交的offset开始进行消费
	partitionConsumer, err := consumer.ConsumePartition(topics, 0, nextOffset+1)
	if err != nil {
		status = 2
		msg = "partitionConsumer create error"
		return status, msg
	}

	defer func() {
		if err = partitionConsumer.Close(); err != nil {
			log.Fatalln(err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			msgdetail := string(msg.Value)
			total := len(msgdetail)
			saveJobToMongo(msgdetail, total)
			//log.Printf("Consumed message offset %d\n message:%s", msg.Offset,string(msg.Value))
			//拿到下一个offset
			nextOffset, _ := partitionOffsetManager.NextOffset()
			//提交offset，默认提交到本地缓存，每秒钟往broker提交一次（可以设置）
			partitionOffsetManager.MarkOffset(nextOffset+1, "modified metadata")
			//nextOffsetnew,_:=partitionOffsetManager.NextOffset()
		case <-signals:
			break ConsumerLoop
		}
	}
	return status, msg
}

//生产者
func SaramaProducerNew(job Logs) error {

	config := kago.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	//groupid := "cg1"
	produ, err := kago.InitManualRetryAsyncProducer([]string{kafkaMap["host"] + ":" + kafkaMap["port"]}, config)
	if err != nil {
		log.Println(err)
		return err
	}
	defer produ.Close()
	go func(p *kago.AsyncProducer) {
		/*for{
			select {
				case suc:=<-p.Successes():
					bytes,_:=suc.Value.Encode()
					value:=string(bytes)
					fmt.Println("offsetCfg:", suc.Offset, " partitions:", suc.Partition," metadata:",suc.Metadata," value:",value)
				case fail := <-p.Errors():
					fmt.Println("err: ", fail.Err)
			}
		} */

		errors := p.Errors()
		success := p.Successes()
		loop:
		for {
			select {
			case <-errors:
				//if err != nil {
				//	fmt.Println("errs: ", err)
				//	break loop
				//}
				break loop
			case <-success:
				break loop
			default:

			}
		}
	}(produ)

	for i := 0; i < 1; i++ {
		jsonString, errs := json.Marshal(job)
		if errs != nil {
			fmt.Println("Failed to get the json of logs: ", errs)
		}
		jobString := string(jsonString)
		// 发送的消息,主题。
		msg := &kago.ProducerMessage{
			Topic: topics,
		}
		msg.Value = sarama.ByteEncoder(jobString)
		//将字符串转化为字节数组

		//使用通道发送
		produ.Send() <- msg
		time.Sleep(500 * time.Millisecond)
	}
	return err
}

//消费者
func SaramaConsumerNew(w *sync.WaitGroup) (status int, msg string) {
	defer w.Done()
	status = 1
	msg = "Consumer success"
	if mongoinit != nil {
		status = 0
		msg = "Mongodb connect fail"
		return status, msg
	}
	config := kago.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.CommitInterval = 1 * time.Second
	config.OffsetLocalOrServer = 1 //0,local  1,server  2,newest
	config.OffsetFileDir = offsetfiledir //配置offset文件路径
	groupid := "cg1"
	consumer, err := kago.InitOneConsumerOfGroup([]string{kafkaMap["host"] + ":" + kafkaMap["port"]}, topics, groupid, config)
	if err != nil {
		log.Println(err)
		status = 2
		msg = "Make consumer fail"
		return status, msg
	}
	partitionList, err := kago.Partitions([]string{kafkaMap["host"] + ":" + kafkaMap["port"]}, topics, config)
	if err != nil {
		fmt.Println("Failed to get the list of partitions: ", err)
		status = 3
		msg = "Get consumer partitions fail"
		return status, msg
	}
	defer consumer.Close()
	kago.InitOffsetFile(config) //初始化offset文件，全局执行一次即可
	go func() {
		for err := range consumer.Errors() {
			log.Printf("Error: %s\n", err.Error())
		}
	}()
	go func() {
		for ntf := range consumer.Notifications() {
			log.Printf("Rebalanced: %+v\n", ntf)
		}
	}()
	for partition := range partitionList {
		pc, errs := kago.InitPartitionConsumer([]string{kafkaMap["host"] + ":" + kafkaMap["port"]}, topics, int32(partition), groupid, config)
		if errs != nil {
			fmt.Printf("Failed to start consumer for partition %d: %s\n", partition, errs)
			status = 4
			pstr := strconv.Itoa(partition)
			msg = "Failed to start consumer for partition " + pstr
			return status, msg
		}
		//defer pc.Close()
		go func(pc *kago.PartitionConsumer) {
			wg.Add(1)
			for msg := range pc.Recv() {
				msgdetail := string(msg.Value)
				total := len(msgdetail)
				saveJobToMongo(msgdetail, total)
				consumer.MarkOffset(msg.Topic, msg.Partition, msg.Offset, groupid, true) // 提交offset，最后一个参数为true时，会将offset保存到文件中
			}
			wg.Done()
		}(pc)
		wg.Wait()
	}
	//consumer.Close()
	return status, msg
}

//手动消费
func DoSaramaConsumerNew() (status int, msg string) {
	status = 1
	msg = "Consumer success"
	if mongoinit != nil {
		status = 0
		msg = "Mongodb connect fail"
		return status, msg
	}
	config := kago.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	config.Consumer.Offsets.CommitInterval = 1 * time.Second
	config.OffsetLocalOrServer = 1 //0,local  1,server  2,newest
	config.OffsetFileDir = offsetfiledir //配置offset文件路径
	groupid := "cg1"
	consumer, err := kago.InitOneConsumerOfGroup([]string{kafkaMap["host"] + ":" + kafkaMap["port"]}, topics, groupid, config)
	if err != nil {
		log.Println(err)
		status = 2
		msg = "Make consumer fail"
		return status, msg
	}
	partitionList, err := kago.Partitions([]string{kafkaMap["host"] + ":" + kafkaMap["port"]}, topics, config)
	if err != nil {
		fmt.Println("Failed to get the list of partitions: ", err)
		status = 3
		msg = "Get consumer partitions fail"
		return status, msg
	}
	defer consumer.Close()
	kago.InitOffsetFile(config) //初始化offset文件，全局执行一次即可
	go func() {
		for err := range consumer.Errors() {
			log.Printf("Error: %s\n", err.Error())
		}
	}()
	go func() {
		for ntf := range consumer.Notifications() {
			log.Printf("Rebalanced: %+v\n", ntf)
		}
	}()
	for partition := range partitionList {
		pc, errs := kago.InitPartitionConsumer([]string{kafkaMap["host"] + ":" + kafkaMap["port"]}, topics, int32(partition), groupid, config)
		if errs != nil {
			fmt.Printf("Failed to start consumer for partition %d: %s\n", partition, errs)
			status = 4
			pstr := strconv.Itoa(partition)
			msg = "Failed to start consumer for partition " + pstr
			return status, msg
		}
		//defer pc.Close()
		go func(pc *kago.PartitionConsumer) {
			wg.Add(1)
			for msg := range pc.Recv() {
				msgdetail := string(msg.Value)
				total := len(msgdetail)
				saveJobToMongo(msgdetail, total)
				consumer.MarkOffset(msg.Topic, msg.Partition, msg.Offset, groupid, true) // 提交offset，最后一个参数为true时，会将offset保存到文件中
			}
			wg.Done()
		}(pc)
	}
	wg.Wait()
	//consumer.Close()
	return status, msg
}

//消息存入mongo
func saveJobToMongo(jobString string, total int) {
	var _job Logs
	b := []byte(jobString)
	errb := json.Unmarshal(b[:total], &_job)
	if errb != nil {
		log.Println(errb)
	}
	_, erri := getLogById(mongoMap, _job.Id, _job.Addtime, 1)
	if erri == nil {
		fmt.Printf("Data allready exist in index table: %s", _job)
	} else {
		collstr := mongoMap["collection"]
		saveJobToIndex(_job, collstr) //插入总表
	}
	_, errl := getLogById(mongoMap, _job.Id, _job.Addtime, 0)
	if errl == nil {
		fmt.Printf("Data allready exist in list table: %s", _job)
	} else {
		collstr := conf.GetCollStr(mongoMap["collection"], _job.Addtime)
		saveJobToList(_job, collstr) //插入分表
	}
}

//消息存入总表
func saveJobToIndex(jobs Logs, collstr string) {
	se := MongoDb.GetSession()
	co := MongoDb.GetCollection(collstr, se)
	defer se.Close()
	err := co.Insert(jobs)
	if err != nil {
		log.Printf("Saved to index MongoDB fail:,%+v\n",err)
	}
	//fmt.Printf("Saved to index MongoDB success: %s", jobs)
}

//消息存入分表
func saveJobToList(jobs Logs, collstr string) {
	se := MongoDb.GetSession()
	co := MongoDb.GetCollection(collstr, se)
	defer se.Close()
	err := co.Insert(jobs)
	if err != nil {
		log.Printf("Saved to list MongoDB fail:,%+v\n",err)
	}
	//fmt.Printf("Saved to list MongoDB success: %s", jobs)
}

//根据id获取日志
func getLogById(m map[string]string, id string, addtime int64, isindex int) (jobs Logs, err error) {

	var collstr string
	if isindex == 1 {
		collstr = mongoMap["collection"]
	} else {
		collstr = conf.GetCollStr(mongoMap["collection"], addtime)
	}
	se := MongoDb.GetSession()
	co := MongoDb.GetCollection(collstr, se)
	defer se.Close()
	err = co.Find(bson.M{"id": id}).One(&jobs)
	return
}
