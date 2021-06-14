package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"log_agent/model"
	"log_agent/utils"
	"os"
	"time"

	"go.etcd.io/etcd/clientv3"
)

//cli etcd连接
var cli *clientv3.Client

//Init 初始化etcd
func Init() {
	var err error
	address := utils.Cfg.Section("etcd").Key("address").String()
	timeout := utils.Cfg.Section("etcd").Key("timeout").MustInt(5)
	cli, err = clientv3.New(clientv3.Config{
		Endpoints:   []string{address},
		DialTimeout: time.Duration(timeout) * time.Second,
	})
	if err != nil {
		utils.Logger.Errorf("connect to etcd failed, err:%v\n", err)
		fmt.Printf("connect to etcd failed, err:%v\n", err)
		os.Exit(1)
	}
}

//GetValue 根据key获取value
func GetValue() ([]*model.LogConf, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	key := utils.Cfg.Section("etcd").Key("log_key").String()
	ipStr := utils.Cfg.Section("local").Key("ip").String()
	key = fmt.Sprintf(key, ipStr)
	resp, err := cli.Get(ctx, key)
	cancel()
	if err != nil {
		return nil, err
	}
	var logConf []*model.LogConf
	for _, ev := range resp.Kvs {
		err := json.Unmarshal(ev.Value, &logConf)
		if err != nil {
			return nil, err
		}
	}
	return logConf, nil
}

//WatchConf 监控配置信息
func WatchConf(key string, newConf chan []*model.LogConf) {
	rch := cli.Watch(context.Background(), key)
	for wresp := range rch {
		for _, ev := range wresp.Events {
			var temp []*model.LogConf
			//不是删除事件
			if ev.Type != clientv3.EventTypeDelete {
				err := json.Unmarshal(ev.Kv.Value, &temp)
				if err != nil {
					utils.Logger.Error("json unmarshal error: ", err)
					continue
				}
			}
			//传递给管道
			newConf <- temp
		}
	}
}
