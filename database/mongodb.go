package database

import (
	"gopkg.in/mgo.v2"
	conf "logs.7gz.com/lib/Func"
)

//mogodb连接
func ConnecToMongo(m map[string]string, addtime int64) *mgo.Collection {
	session, err := mgo.Dial(m["host"] + ":" + m["port"])
	if err != nil {
		panic(err)
	}
	//defer session.Close()
	var collstr string
	if addtime == 0 {
		collstr = m["collection"]
	} else {
		collstr = conf.GetCollStr(m["collection"], addtime)
	}
	session.SetMode(mgo.Monotonic, true)
	c := session.DB(m["db"]).C(collstr)
	return c
}

//mysql连接
