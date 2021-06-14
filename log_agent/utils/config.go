package utils

import (
	"fmt"
	"os"

	"gopkg.in/ini.v1"
)

//Cfg 配置文件对象
var Cfg *ini.File

//InitCfg 初始化config
func InitCfg() {
	var err error
	Cfg, err = ini.Load("config/config.ini")
	if err != nil {
		fmt.Printf("Fail to read file: %v\n", err)
		os.Exit(1)
	}
}
