package main

import (
	"fmt"
	"log_agent/etcd"
	"log_agent/kafka"
	"log_agent/tail"
	"log_agent/utils"
	"os"
)

func init() {
	//1.初始化配置文件
	utils.InitCfg()

	//2.初始化日志
	utils.InitLog()

	//3.初始化kafka
	kafka.Init()

	//4.初始化etcd
	etcd.Init()

	//5.保存本地ip(静态ip)
	ip := utils.Cfg.Section("local").Key("ip").String()
	if ip == "" {
		ipStr, err := utils.GetLocalIP()
		if err != nil {
			utils.Logger.Error("get local ip error: ", err)
			fmt.Println("get local ip error: ", err)
			os.Exit(1)
		}
		utils.Cfg.Section("local").Key("ip").SetValue(ipStr)
		err = utils.Cfg.SaveTo("config/config.ini")
		if err != nil {
			utils.Logger.Error("save config file error: ", err)
			fmt.Println("save config file error: ", err)
			os.Exit(1)
		}
	}
}

func main() {
	//获取etcd上的配置信息
	logConf, err := etcd.GetValue()
	if err != nil {
		utils.Logger.Errorf("get from etcd failed, err:%v\n", err)
		fmt.Printf("get from etcd failed, err:%v\n", err)
		os.Exit(1)
	}

	//新建tail任务
	tail.InitTaskManager(logConf)

	//阻塞
	select {}
}
