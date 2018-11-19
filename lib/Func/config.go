package conf

import (
	"bufio"
	"github.com/sony/sonyflake"
	"io"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	machineID     int64 // 机器 id 占10位, 十进制范围是 [ 0, 1023 ]
	sn            int64 // 序列号占 12 位,十进制范围是 [ 0, 4095 ]
	lastTimeStamp int64 // 上次的时间戳(毫秒级), 1秒=1000毫秒, 1毫秒=1000微秒,1微秒=1000纳秒
)

func init() {
	lastTimeStamp = time.Now().UnixNano() / 1000000
}

func SetMachineId(mid int64) {
	// 把机器 id 左移 12 位,让出 12 位空间给序列号使用
	machineID = mid << 12
}

func GetSnowflakeId() int64 {
	curTimeStamp := time.Now().UnixNano() / 1000000

	// 同一毫秒
	if curTimeStamp == lastTimeStamp {
		sn++
		// 序列号占 12 位,十进制范围是 [ 0, 4095 ]
		if sn > 4095 {
			time.Sleep(time.Millisecond)
			curTimeStamp = time.Now().UnixNano() / 1000000
			lastTimeStamp = curTimeStamp
			sn = 0
		}

		// 取 64 位的二进制数 0000000000 0000000000 0000000000 0001111111111 1111111111 1111111111  1 ( 这里共 41 个 1 )和时间戳进行并操作
		// 并结果( 右数 )第 42 位必然是 0,  低 41 位也就是时间戳的低 41 位
		rightBinValue := curTimeStamp & 0x1FFFFFFFFFF
		// 机器 id 占用10位空间,序列号占用12位空间,所以左移 22 位; 经过上面的并操作,左移后的第 1 位,必然是 0
		rightBinValue <<= 22
		id := rightBinValue | machineID | sn
		return id
	}

	if curTimeStamp > lastTimeStamp {
		sn = 0
		lastTimeStamp = curTimeStamp
		// 取 64 位的二进制数 0000000000 0000000000 0000000000 0001111111111 1111111111 1111111111  1 ( 这里共 41 个 1 )和时间戳进行并操作
		// 并结果( 右数 )第 42 位必然是 0,  低 41 位也就是时间戳的低 41 位
		rightBinValue := curTimeStamp & 0x1FFFFFFFFFF
		// 机器 id 占用10位空间,序列号占用12位空间,所以左移 22 位; 经过上面的并操作,左移后的第 1 位,必然是 0
		rightBinValue <<= 22
		id := rightBinValue | machineID | sn
		return id
	}

	if curTimeStamp < lastTimeStamp {
		return 0
	}

	return 0
}

func InitConfig(path string) map[string]string {
	//初始化
	myMap := make(map[string]string)

	//打开文件指定目录，返回一个文件f和错误信息
	f, err := os.Open(path)

	//异常处理 以及确保函数结尾关闭文件流
	if err != nil {
		panic(err)
	}
	defer f.Close()

	//创建一个输出流向该文件的缓冲流*Reader
	r := bufio.NewReader(f)
	for {
		//读取，返回[]byte 单行切片给b
		b, _, err := r.ReadLine()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}

		//去除单行属性两端的空格
		s := strings.TrimSpace(string(b))
		//fmt.Println(s)

		//判断等号=在该行的位置
		index := strings.Index(s, "=")
		if index < 0 {
			continue
		}
		//取得等号左边的key值，判断是否为空
		key := strings.TrimSpace(s[:index])
		if len(key) == 0 {
			continue
		}

		//取得等号右边的value值，判断是否为空
		value := strings.TrimSpace(s[index+1:])
		if len(value) == 0 {
			continue
		}
		//这样就成功吧配置文件里的属性key=value对，成功载入到内存中c对象里
		myMap[key] = value
	}
	return myMap
}

//生成唯一id
func MadeNumUuid(month int, num int) string {
	second := time.Now().Second()
	secondstr := strconv.Itoa(second) //秒
	if second < 10 {
		secondstr = "0" + secondstr
	}
	//timeUnix := time.Now().Nanosecond() //纳秒
	timeUnix := time.Now().UnixNano() / 1e5 //纳秒
	rand.Seed(time.Now().Unix())
	var millsencond string
	//newtimeUnix := strconv.Itoa(timeUnix)
	newtimeUnix := strconv.FormatInt(timeUnix, 10)
	if num == 9 {
		millsencond = Substr(newtimeUnix, -3, 3)
	} else {
		millsencond = Substr(newtimeUnix, -3, 4)
	}
	randnum := strconv.Itoa(rand.Intn(89) + 10)
	newmonth := strconv.Itoa(month)
	if month < 10 {
		newmonth = "0" + newmonth
	}
	uuid := randnum + secondstr + millsencond + newmonth
	return uuid
}

//截取字符串 start 起点下标 length 需要截取的长度
func Substr(str string, start int, length int) string {
	rs := []rune(str)
	rl := len(rs)
	end := 0
	if start < 0 {
		start = rl - 1 + start
	}
	end = start + length
	if start > end {
		start, end = end, start
	}
	if start < 0 {
		start = 0
	}
	if start > rl {
		start = rl
	}
	if end < 0 {
		end = 0
	}
	if end > rl {
		end = rl
	}
	return string(rs[start:end])
}

func GenSonyflake() uint64 {
	flake := sonyflake.NewSonyflake(sonyflake.Settings{})
	id, err := flake.NextID()
	if err != nil {
		//log.Fatalf("flake.NextID() failed with %s\n", err)
	}
	// Note: this is base16, could shorten by encoding as base62 string
	//fmt.Printf("github.com/sony/sonyflake: %x\n", id)
	return id
}

//获取集合下标
func GetCollStr(precollstr string, addtime int64) string {
	if addtime == 0 || precollstr == "" {
		return ""
	}
	monthstr := time.Unix(addtime, 0).Month()
	yearstr := time.Unix(addtime, 0).Year()

	month := int(monthstr)
	year := int(yearstr)
	newmonth := strconv.Itoa(month)
	newyear := strconv.Itoa(year)
	if month < 10 {
		newmonth = "0" + newmonth
	}
	str := newyear + newmonth
	returnstr := precollstr + "_" + str
	return returnstr
}

// 判断文件夹是否存在
func PathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// 创建文件夹
func OsMakeDir(dir string) (bool, error) {
	err := os.MkdirAll(dir, os.ModePerm)
	if err == nil {
		return true, nil
	}
	return false, err
}

// 获取kafka偏移量记录文件目录
func GetKafkaOffsetDir(orpath string) string {
	pathsrt, _ := os.Getwd()
	offsetfiledir := pathsrt + orpath
	exist, _ := PathExists(offsetfiledir)
	if !exist {
		result,err := OsMakeDir(offsetfiledir)
		if !result {
			log.Println(err)
		}
	}
	return offsetfiledir
}