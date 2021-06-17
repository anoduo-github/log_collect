package es

import (
	"context"
	"fmt"
	"log_transfor/model"
	"log_transfor/utils"
	"os"
	"time"

	"github.com/olivere/elastic/v7"
)

var (
	client *elastic.Client     //client es连接
	dataCh chan *model.LogData //日志信息管道
)

//Init 初始化
func Init() {
	host := utils.Cfg.Section("es").Key("address").String()
	var err error
	client, err = elastic.NewClient(elastic.SetURL(host))
	if err != nil {
		utils.Logger.Error("new elastic error: ", err)
		fmt.Println("new elastic error: ", err)
		os.Exit(1)
	}
	max_chan_size := utils.Cfg.Section("es").Key("max_chan_size").MustInt(1000)
	dataCh = make(chan *model.LogData, max_chan_size)
}

//SaveToChan 保存到chan
func SaveToChan(data *model.LogData) {
	dataCh <- data
}

//sendToEs 发送给es
func sendToEs() {
	for {
		select {
		case msg := <-dataCh:
			_, err := client.Index().
				Index(msg.Topic).
				BodyJson(msg.Data).
				Do(context.Background())
			if err != nil {
				utils.Logger.Error("send to es error: ", err)
			}
		default:
			time.Sleep(time.Millisecond * 1000)
		}
	}
}

//Run 执行es发送操作
func Run() {
	max_goroutine_nums := utils.Cfg.Section("es").Key("max_goroutine_nums").MustInt(50)
	for i := 1; i <= max_goroutine_nums; i++ {
		go sendToEs()
	}
}
