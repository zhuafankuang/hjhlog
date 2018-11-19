package apis

import (
	"expvar"
	"fmt"
	"github.com/gin-gonic/gin"
	"html"
	"log"
	"logs.7gz.com/lib/Func"
	"logs.7gz.com/lib/Queue"
	. "logs.7gz.com/models"
	"net/http"
	"strconv"
	"sync"
	"time"
)

var (
//导入配置文件
//dbMap = conf.InitConfig("conf/db_conf.txt")
)

type AddLoginfo struct {
	Source string `form:"source" binding:"required"`
	Bu     string `form:"bu"`
	Type   int    `form:"type" binding:"required"`
	Level  int    `form:"level" binding:"required"`
	//Status int `form:"status"`
	Contents string `form:"contents" binding:"required"`
	Addtime  int64  `form:"addtime"`
	Ip       string `form:"ip"`
}

type EditLoginfo struct {
	Id     string `form:"id" binding:"required"`
	Source string `form:"source"`
	Bu     string `form:"bu"`
	Type   int    `form:"type"`
	Level  int    `form:"level"`
	//Status int `form:"status"`
	Contents string `form:"contents"`
	Addtime  int64  `form:"addtime"`
	Ip       string `form:"ip"`
}

type ConditionLoginfo struct {
	Id     string `form:"id"`
	Source string `form:"source"`
	Bu     string `form:"bu"`
	Type   int    `form:"type"`
	Level  int    `form:"level"`
	//Status int `form:"status"`
	Contents string `form:"contents"`
	Addtime  int64  `form:"addtime"`
	Ip       string `form:"ip"`
}

func IndexLog(c *gin.Context) {
	c.String(http.StatusOK, "Welcome to leju logs api system！Last update time:2018-11-07 17:00！")
}

//添加日志
func AddLog(c *gin.Context) {
	var logs AddLoginfo
	if err := c.ShouldBind(&logs); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status": 0,
			"msg":    err.Error(),
		})
		return
	}
	if logs.Source == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"status": 3,
			"msg":    "source cannot be empty",
		})
		return
	}
	if logs.Type == 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"status": 3,
			"msg":    "type cannot be empty",
		})
		return
	}
	if logs.Level == 0 {
		c.JSON(http.StatusBadRequest, gin.H{
			"status": 3,
			"msg":    "level cannot be empty",
		})
		return
	}
	if logs.Contents == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"status": 3,
			"msg":    "contents cannot be empty",
		})
		return
	}

	if logs.Addtime == 0 {
		logs.Addtime = time.Now().Unix()
	} else {
		timestr := strconv.FormatInt(logs.Addtime, 10)
		tlen := len(timestr)
		if tlen > 10 {
			c.JSON(http.StatusBadRequest, gin.H{
				"status": 3,
				"msg":    "type is wrong for addtime",
			})
		}
	}
	id := conf.GetSnowflakeId()
	ids := strconv.FormatInt(id, 10)
	loginfo := resque.Logs{Id: ids, Source: html.EscapeString(logs.Source), Bu: html.EscapeString(logs.Bu), Type: logs.Type, Level: logs.Level, Contents: html.EscapeString(logs.Contents), Addtime: logs.Addtime, Ip: html.EscapeString(logs.Ip)}
	status := resque.Producer(loginfo)
	if status != 1 {
		c.JSON(http.StatusOK, gin.H{
			"status": 2,
			"msg":    "Kafka produce fail",
		})
	} else {
		c.JSON(http.StatusOK, gin.H{
			"status": 1,
			"id":     id,
			"msg":    "sucsses",
		})
	}
}

//消费者消费消息
func DoCoonsumer(c *gin.Context) {
	status, msg := resque.DoConsumer()
	c.JSON(http.StatusOK, gin.H{
		"status": status,
		"msg":    msg,
	})
}

//消费者消费消息
func Coonsumer(w *sync.WaitGroup) {
	//defer w.Done()
	_, msg := resque.Consumer(w)
	fmt.Println(msg)
}

//分页获取日志列表
func GetLogList(c *gin.Context) {
	var mypage int
	var mylimit int
	var msg string
	page := c.DefaultQuery("page", "1")
	limit := c.DefaultQuery("limit", "50")
	if page == "" {
		mypage = 1
	} else {
		mypage, _ = strconv.Atoi(page)
	}
	if limit == "" {
		mylimit = 50
	} else {
		mylimit, _ = strconv.Atoi(limit)
	}
	if mypage <= 0 {
		mypage = 1
	}
	if mylimit <= 0 {
		mylimit = 50
	}
	lejulogs := LejuLogs{}
	logs, status := lejulogs.GetLogList(mypage, mylimit)

	if status != 1 {
		if status == 0 {
			msg = "Mongodb connect fail"
		} else if status == 2 {
			msg = "No data is available"
		}
		c.JSON(http.StatusOK, gin.H{
			"status": status,
			"msg":    msg,
		})
	} else {
		c.JSON(http.StatusOK, gin.H{
			"loglist": logs,
			"status":  status,
		})
	}
}

//获取条件获取所有日志列表
func GetLogByCondition(c *gin.Context) {
	var logs ConditionLoginfo
	if err := c.ShouldBind(&logs); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status": 3,
			"msg":    err.Error(),
		})
		return
	}
	lejulogs := LejuLogs{}
	var msg string
	lejulogs = LejuLogs{Id: html.EscapeString(logs.Id), Source: html.EscapeString(logs.Source), Bu: html.EscapeString(logs.Bu), Type: logs.Type, Level: logs.Level, Contents: html.EscapeString(logs.Contents), Addtime: logs.Addtime, Ip: html.EscapeString(logs.Ip)}
	row, stat := lejulogs.GetLogByCondition()
	if stat != 1 {
		if stat == 0 {
			msg = "Mongodb connect fail"
		} else if stat == 2 {
			msg = "No data is available"
		}
		c.JSON(http.StatusOK, gin.H{
			"status": stat,
			"msg":    msg,
		})
	} else {
		c.JSON(http.StatusOK, gin.H{
			"status":  stat,
			"loglist": row,
		})
	}

}

//获取单条日志
func GetLogById(c *gin.Context) {
	logs := LejuLogs{}
	id := c.Param("id")
	if id != "" {
		logs = LejuLogs{Id: id}
	} else {
		c.JSON(http.StatusOK, gin.H{
			"status": 3,
			"msg":    "id cannot be empty",
		})
		return
	}
	var msg string
	row, status := logs.GetLogById()
	if status != 1 {
		if status == 0 {
			msg = "Mongodb connect fail"
		} else if status == 2 {
			msg = "Log is not exist"
		}
		c.JSON(http.StatusOK, gin.H{
			"status": status,
			"msg":    msg,
		})
	} else {
		c.JSON(http.StatusOK, gin.H{
			"status":  status,
			"loginfo": row,
		})
	}
}

//修改日志
func EditLog(c *gin.Context) {
	var logs EditLoginfo
	if err := c.ShouldBind(&logs); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"status": 4,
			"msg":    err.Error(),
		})
		return
	}
	mlogs := LejuLogs{}
	var msg string
	id := c.Request.FormValue("id")
	var logid, log_source, log_bu, log_contents, log_ip string
	var log_t, log_level int
	var log_tm int64
	hasparam := 0
	if id != "" {
		mlogs = LejuLogs{Id: id}
		logid = id
	} else {
		msg = "id cannot be empty"
		c.JSON(http.StatusOK, gin.H{
			"status": 5,
			"msg":    msg,
		})
		return
	}
	row, status := mlogs.GetLogById()
	if status != 1 {
		if status == 0 {
			msg = "mongodb connect fail"
		} else if status == 2 {
			msg = "log is not exist"
		}
		c.JSON(http.StatusOK, gin.H{
			"status": status,
			"msg":    msg,
		})
		return
	} else {
		source := logs.Source
		if source != "" {
			hasparam = 1
			log_source = html.EscapeString(source)
		} else {
			log_source = row.Source
		}
		bu := logs.Bu
		if bu != "" {
			hasparam = 1
			log_bu = html.EscapeString(bu)
		} else {
			log_bu = row.Bu
		}
		log_type := logs.Type
		if log_type != 0 {
			hasparam = 1
			log_t = log_type
		} else {
			log_t = row.Type
		}
		level := logs.Level
		if level != 0 {
			hasparam = 1
			log_level = level
		} else {
			log_level = row.Level
		}
		//status := logs.Status
		//if status != 0 {
		//	hasparam = 1
		//	log_status = status
		//}else{
		//	log_status = row.Status
		//}
		contents := logs.Contents
		if contents != "" {
			hasparam = 1
			log_contents = html.EscapeString(contents)
		} else {
			log_contents = row.Contents
		}
		log_time := logs.Addtime
		if log_time != 0 {
			hasparam = 1
			log_tm = log_time
		} else {
			log_tm = row.Addtime
		}
		//ip := c.Request.FormValue("ip")
		ip := logs.Ip
		if log_ip != "" {
			hasparam = 1
			log_ip = html.EscapeString(ip)
		} else {
			log_ip = row.Ip
		}
	}
	if hasparam == 0 {
		c.JSON(http.StatusOK, gin.H{
			"status": 3,
			"msg":    "no params to update",
		})
		return
	}

	lejulogs := LejuLogs{Id: logid, Source: log_source, Bu: log_bu, Type: log_t, Level: log_level, Contents: log_contents, Addtime: log_tm, Ip: log_ip}
	err := c.Bind(&lejulogs)
	if err != nil {
		log.Fatalln(err)
		c.JSON(http.StatusOK, gin.H{
			"status": 6,
			"msg":    err,
		})
	} else {
		msg := lejulogs.UpdateLog()
		if msg != nil {
			c.JSON(http.StatusOK, gin.H{
				"status": 6,
				"msg":    msg,
			})
		} else {
			c.JSON(http.StatusOK, gin.H{
				"status": 1,
				"msg":    "sucsses",
			})
		}
	}
}

//删除日志
func DelLog(c *gin.Context) {
	var msg string
	id := c.Param("id")
	if id == "" {
		msg = "id cannot be empty"
		c.JSON(http.StatusOK, gin.H{
			"status": 3,
			"msg":    msg,
		})
		return
	}
	lejulogs := LejuLogs{Id: id}
	row, status := lejulogs.GetLogById()
	if status != 1 {
		if status == 0 {
			msg = "Mongodb connect fail"
		} else if status == 2 {
			msg = "Log is not exist"
		}
		c.JSON(http.StatusOK, gin.H{
			"status": status,
			"msg":    msg,
		})
		return
	} else {
		lejulogs = LejuLogs{Id: id, Addtime: row.Addtime}
	}
	err := lejulogs.DelLog()
	if err != nil {
		c.JSON(http.StatusOK, gin.H{
			"status": 4,
			"msg":    err,
		})
	} else {
		c.JSON(http.StatusOK, gin.H{
			"status": 1,
			"msg":    "sucsses",
		})
	}
}
func MetricsHandler(c *gin.Context) {
	w := c.Writer
	w.Header().Set("Content-Type", "application/json; charset=utf-8")

	first := true
	report := func(key string, value interface{}) {
		if !first {
			fmt.Fprintf(w, ",\n")
		}
		first = false
		if str, ok := value.(string); ok {
			fmt.Fprintf(w, "%q: %q", key, str)
		} else {
			fmt.Fprintf(w, "%q: %v", key, value)
		}
	}

	fmt.Fprintf(w, "{\n")
	expvar.Do(func(kv expvar.KeyValue) {
		report(kv.Key, kv.Value)
	})
	fmt.Fprintf(w, "\n}\n")
}
