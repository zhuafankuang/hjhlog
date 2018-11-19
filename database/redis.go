package database

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
)

//redis连接
func ConnecToRedis(m map[string]string) redis.Conn {
	c, err := redis.Dial("tcp", m["host"]+":"+m["port"])
	if err != nil {
		fmt.Println("Connect to redis error", err)
	}
	return c
}