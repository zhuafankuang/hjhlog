package models

import (
	"gopkg.in/mgo.v2/bson"
	"log"

	MongoDb "logs.7gz.com/database"
	//Redis "logsapi.7gz.com/database"
	conf "logs.7gz.com/lib/Func"
)

var (
	//导入配置文件
	mongoMap = conf.InitConfig("conf/mongo_conf.txt")
	//redisMap = conf.InitConfig("conf/redis_conf.txt")
	mongoinit = MongoDb.MongoInit()
)

type LejuLogs struct {
	Id string  `json:"id"`
	Source string  `json:"source"`
	Bu string  `json:"bu"`
	Type int  `json:"type"`
	Level int  `json:"level"`
	//Status int  `json:"status"`
	Contents string  `json:"contents"`
	Addtime int64  `json:"addtime"`
	Ip string  `json:"ip"`
}

//添加日志
func (s *LejuLogs) AddLogMongo() (err error) {
	var collstr string
	if s.Addtime == 0 {
		collstr = mongoMap["collection"]
	} else {
		collstr = conf.GetCollStr(mongoMap["collection"], s.Addtime)
	}
	se := MongoDb.GetSession()
	co := MongoDb.GetCollection(collstr, se)
	defer se.Close()
	err = co.Insert(s) //插入数据
	//c := MongoDb.ConnecToMongo(mongoMap,s.Id)
	//err = c.Insert(s)
	if err != nil {
		log.Fatal(err)
	}
	return
}

//获取日志列表
func (s *LejuLogs) GetLogList(page int, limit int) (lejulogs []LejuLogs, status int) {
	status = 1
	if mongoinit != nil {
		status = 0
		return lejulogs, status
	}
	start := (page - 1) * limit
	//c := MongoDb.ConnecToMongo(mongoMap,"")
	var collstr string
	if s.Addtime == 0 {
		collstr = mongoMap["collection"]
	} else {
		collstr = conf.GetCollStr(mongoMap["collection"], s.Addtime)
	}
	se := MongoDb.GetSession()
	co := MongoDb.GetCollection(collstr, se)
	defer se.Close()

	err := co.Find(nil).Skip(start).Limit(limit).All(&lejulogs)
	/*r := Redis.ConnecToRedis(redisMap)
	_,errs := r.Do("SET", "leju_log_key", "test log go")
	if errs != nil {
		log.Fatal(errs)
	}*/
	if err != nil {
		status = 2
	}
	return lejulogs, status
}

//根据id获取日志
func (s *LejuLogs) GetLogById() (lejulogs LejuLogs, status int) {
	status = 1
	if mongoinit != nil {
		status = 0
		return lejulogs, status
	}
	var collstr string
	if s.Addtime == 0 {
		collstr = mongoMap["collection"]
	} else {
		collstr = conf.GetCollStr(mongoMap["collection"], s.Addtime)
	}
	se := MongoDb.GetSession()
	co := MongoDb.GetCollection(collstr, se)
	defer se.Close()
	//c := MongoDb.ConnecToMongo(mongoMap,"")
	err := co.Find(bson.M{"id": s.Id}).One(&lejulogs)
	if err != nil {
		status = 2
	}
	return lejulogs, status
}

//根据条件获取日志
func (s *LejuLogs) GetLogByCondition() (lejulogs []LejuLogs, status int) {

	status = 1
	if mongoinit != nil {
		status = 0
		return lejulogs, status
	}
	match := bson.M{}
	if s.Id != "" {
		match["id"] = s.Id
	}
	if s.Source != "" {
		match["source"] = s.Source
	}
	if s.Bu != "" {
		match["bu"] = s.Bu
	}
	if s.Type != 0 {
		match["type"] = s.Type
	}
	if s.Level != 0 {
		match["level"] = s.Level
	}
	//if s.Status != 0 {
	//	match["status"] = s.Status
	//}
	if s.Contents != "" {
		match["contents"] = s.Contents
	}
	if s.Addtime != 0 {
		match["addtime"] = s.Addtime
	}
	if s.Ip != "" {
		match["ip"] = s.Ip
	}

	var collstr string
	if s.Addtime == 0 {
		collstr = mongoMap["collection"]
	} else {
		collstr = conf.GetCollStr(mongoMap["collection"], s.Addtime)
	}
	se := MongoDb.GetSession()
	co := MongoDb.GetCollection(collstr, se)
	defer se.Close()

	//c := MongoDb.ConnecToMongo(mongoMap,s.Addtime)
	err := co.Find(match).All(&lejulogs)
	if err != nil {
		status = 2
	}
	return lejulogs, status
}

//更新日志
func (s *LejuLogs) UpdateLog() (err error) {
	c := MongoDb.ConnecToMongo(mongoMap, s.Addtime)
	errs := c.Update(bson.M{"id": s.Id}, bson.M{"$set": bson.M{"source": s.Source, "bu": s.Bu, "type": s.Type, "level": s.Level, "contents": s.Contents, "addtime": s.Addtime, "ip": s.Ip}})
	if errs != nil {
		log.Fatal(errs)
	}
	i := MongoDb.ConnecToMongo(mongoMap, 0)
	err = i.Update(bson.M{"id": s.Id}, bson.M{"$set": bson.M{"source": s.Source, "bu": s.Bu, "type": s.Type, "level": s.Level, "contents": s.Contents, "addtime": s.Addtime, "ip": s.Ip}})
	if err != nil {
		log.Fatal(err)
	}
	return
}

//删除日志
func (s *LejuLogs) DelLog() (err error) {
	c := MongoDb.ConnecToMongo(mongoMap, s.Addtime)
	_, errs := c.RemoveAll(bson.M{"id": s.Id})
	if errs != nil {
		log.Fatal(errs)
	}
	i := MongoDb.ConnecToMongo(mongoMap, 0)
	_, err = i.RemoveAll(bson.M{"id": s.Id})
	if err != nil {
		log.Fatal(err)
	}
	return
}
