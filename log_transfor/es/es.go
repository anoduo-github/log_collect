package es

import (
	"fmt"
	"log_transfor/utils"
	"os"

	"github.com/olivere/elastic/v7"
)

//client es连接
var client *elastic.Client

//var host string

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
}
