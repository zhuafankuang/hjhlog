package main

import (
	"github.com/DeanThompson/ginpprof"
	"github.com/gin-gonic/gin"
	. "logs.7gz.com/apis"
)

func initRouter() *gin.Engine {
	//路由
	router := gin.Default()
	router.GET("/", IndexLog)
	router.POST("/addlog", AddLog)
	router.POST("/alllogs", GetLogList)
	router.GET("/loginfo/:id", GetLogById)
	router.POST("/loglist", GetLogByCondition)
	router.POST("/editlog", EditLog)
	router.DELETE("/dellog/:id", DelLog)
	router.GET("/consumer", DoCoonsumer)

	router.GET("/debug/vars", MetricsHandler)

	ginpprof.Wrapper(router)
	return router
}
