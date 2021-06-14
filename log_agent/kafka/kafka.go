package kafka

import (
	"fmt"
	"os"
	"time"

	"log_agent/model"
	"log_agent/utils"

	"github.com/Shopify/sarama"
)

var (
	//client kafka生产端
	client sarama.SyncProducer

	//日志信息管道
	logChan chan *model.LogData
)

//Init 初始化kafka
func Init() {
	//进行配置
	conf := sarama.NewConfig()
	conf.Producer.RequiredAcks = sarama.WaitForAll          //发送完数据需要leader和follow都确认
	conf.Producer.Partitioner = sarama.NewRandomPartitioner //新选出一个partition
	conf.Producer.Return.Successes = true                   //成功交付的消息将在success channel返回

	//获取配置文件信息
	address := utils.Cfg.Section("kafka").Key("address").String()
	//连接kafka
	var err error
	client, err = sarama.NewSyncProducer([]string{address}, conf)
	if err != nil {
		utils.Logger.Error("kafka init error: ", err)
		fmt.Println("kafka init error: ", err)
		os.Exit(1)
	}

	//初始化管道
	maxsize := utils.Cfg.Section("kafka").Key("maxsize").MustInt(1000)
	logChan = make(chan *model.LogData, maxsize)

	//启动传递功能
	go msgToKafka()
}

//msgToKafka 将信息传递给kafka
func msgToKafka() {
	for {
		select {
		case logData := <-logChan:
			//构造一个消息
			pm := &sarama.ProducerMessage{}
			pm.Topic = logData.Topic
			pm.Value = sarama.StringEncoder(logData.Data)

			//发送
			_, _, err := client.SendMessage(pm)
			if err != nil {
				utils.Logger.Error("send message to kafka error: ", err)
			}
		default:
			time.Sleep(time.Millisecond * 1000)
		}
	}
}

//SendToChan 发送给管道
func SendToChan(topic, data string) {
	logData := &model.LogData{
		Topic: topic,
		Data:  data,
	}
	logChan <- logData
}
