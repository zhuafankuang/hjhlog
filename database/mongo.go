package database

import (
	"github.com/go-mgo/mgo"
	"github.com/wonderivan/logger"

	conf "logs.7gz.com/lib/Func"
)

var (
	globalSession = new(mgo.Session)
	mongoMap      = conf.InitConfig("conf/mongo_conf.txt")
)

//mogodb连接
func MongoInit() error {
	var err error
	var url string
	//url = "mongodb://" + mongoMap["user"] + ":" + mongoMap["pass"] + "@" + mongoMap["host"] + ":" + mongoMap["port"] + "/" + mongoMap["db"]
	url = mongoMap["host"] + ":" + mongoMap["port"] + "/" + mongoMap["db"]
	globalSession, err = mgo.Dial(url)
	if err != nil {
		logger.Error("mongo init error", err)
		return err
	}
	globalSession.SetMode(mgo.Strong, true)
	return err
}

func GetCollection(collectionName string, se *mgo.Session) *mgo.Collection {
	return se.DB("").C(collectionName)
}

func GetDb(se *mgo.Session) *mgo.Database {
	return se.DB("")
}

func GetSession() *mgo.Session {
	return globalSession.Copy()
}

//mysql连接
