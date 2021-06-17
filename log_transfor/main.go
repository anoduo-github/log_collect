package main

import (
	"log_transfor/es"
	"log_transfor/kafka"
	"log_transfor/utils"
)

func init() {
	//初始化配置文件
	utils.InitCfg()

	//初始化日志功能
	utils.InitLog()

	//初始化es
	es.Init()

	//初始化kafka
	kafka.Init()
}

func main() {
	//运行es
	go es.Run()

	//运行kafka
	kafka.Run()
}
