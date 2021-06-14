package tail

import (
	"context"
	"fmt"
	"log_agent/etcd"
	"log_agent/kafka"
	"log_agent/model"
	"log_agent/utils"
	"time"

	"github.com/nxadm/tail"
)

//TailTask tail任务
type TailTask struct {
	TailObj    *tail.Tail         //tail对象
	Path       string             //需要收集的日志路径
	Topic      string             //需要发送到kafka对应的topic
	ctx        context.Context    //上下文
	cancelFunc context.CancelFunc //退出方法
}

//TailManager tail管理
type TailManager struct {
	Conf      []*model.LogConf      //需要收集的日志配置信息
	TaskMap   map[string]*TailTask  //tail任务map
	TaskCount int                   //tail任务总数
	NewConfCh chan []*model.LogConf //修改得新日志配置信息
}

//init 初始化tail
func (t *TailTask) init() {
	//配置
	config := tail.Config{
		ReOpen:    true,                                 //true则文件被删掉阻塞等待新建该文件，false则文件被删掉时程序结束
		Follow:    true,                                 //true则一直阻塞并监听指定文件，false则一次读完就结束程序
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2}, // 指定开始读取的位置
		MustExist: false,                                //true则没有找到文件就报错并结束，false则没有找到文件就阻塞保持住
		Poll:      true,                                 //使用Linux的Poll函数，poll的作用是把当前的文件指针挂到等待队列
	}

	//获得tail对象
	var err error
	t.TailObj, err = tail.TailFile(t.Path, config)
	if err != nil {
		utils.Logger.Error("tail init error: ", err)
		return
	}

	//启动tail
	go t.run()
}

//run 运行tail
func (t *TailTask) run() {
	for {
		select {
		case <-t.ctx.Done():
			utils.Logger.Infof("%s_%s task is closed", t.Topic, t.Path)
			return
		case msg := <-t.TailObj.Lines:
			kafka.SendToChan(t.Topic, msg.Text)
		default:
			time.Sleep(time.Millisecond * 1000)
		}
	}
}

//newTailTask 新建TailTask
func newTailTask(path, topic string) *TailTask {
	ctx1, cancel := context.WithCancel(context.Background())
	tailTask := new(TailTask)
	tailTask.Path = path
	tailTask.Topic = topic
	tailTask.ctx = ctx1
	tailTask.cancelFunc = cancel
	tailTask.init()
	return tailTask
}

//InitTaskManager 初始化TaskManager
func InitTaskManager(conf []*model.LogConf) *TailManager {
	t := new(TailManager)
	t.Conf = conf
	t.TaskMap = make(map[string]*TailTask)
	t.NewConfCh = make(chan []*model.LogConf, 1)
	for _, v := range conf {
		task := newTailTask(v.Path, v.Topic)
		key := fmt.Sprintf("%s_%s", v.Topic, v.Path)
		t.TaskMap[key] = task
		t.TaskCount++
	}
	key := utils.Cfg.Section("etcd").Key("log_key").String()
	ipStr := utils.Cfg.Section("local").Key("ip").String()
	key = fmt.Sprintf(key, ipStr)
	//监听etcd日志配置信息
	go etcd.WatchConf(key, t.NewConfCh)
	//获取修改的日志配置信息
	go t.run()
	return t
}

//run 获取管道信息
func (t *TailManager) run() {
	for {
		select {
		case newConf := <-t.NewConfCh:
			//新增
			for _, v := range newConf {
				tempKey := fmt.Sprintf("%s_%s", v.Topic, v.Path)
				_, ok := t.TaskMap[tempKey]
				if !ok {
					newTT := newTailTask(v.Path, v.Topic)
					t.TaskMap[tempKey] = newTT
				}
			}
			//删除
			for _, v := range t.Conf {
				var tag = true
				for _, n := range newConf {
					if v.Topic == n.Topic && v.Path == n.Path {
						tag = false
						break
					}
				}
				if tag {
					key := fmt.Sprintf("%s_%s", v.Topic, v.Path)
					if _, ok := t.TaskMap[key]; ok {
						t.TaskMap[key].cancelFunc()
					}
				}
			}
		default:
			time.Sleep(time.Millisecond * 500)
		}
	}
}
