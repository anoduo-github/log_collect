package kafka

import (
	"context"
	"fmt"
	"log_transfor/es"
	"log_transfor/model"
	"log_transfor/utils"
	"os"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
)

//client kafka消费组客户端
var client sarama.ConsumerGroup

//Init 初始化kafka连接
func Init() {
	config := sarama.NewConfig()
	//kafka版本
	config.Version = sarama.V0_10_2_0
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	address := utils.Cfg.Section("kafka").Key("address").String()
	group := utils.Cfg.Section("kafka").Key("group").String()

	var err error
	client, err = sarama.NewConsumerGroup([]string{address}, group, config)
	if err != nil {
		utils.Logger.Errorf("Error creating consumer group client: %v", err)
		fmt.Printf("Error creating consumer group client: %v\n", err)
		os.Exit(1)
	}

}

//Run 执行kafka消费
func Run() {
	//获取kafka消费主题
	topics := utils.Cfg.Section("kafka").Key("topics").String()

	ctx, cancel := context.WithCancel(context.Background())

	consumer := Consumer{
		ready: make(chan bool),
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		for {
			//应该在无限循环中调用consue,当服务器端发生重新平衡时，需要重新创建消费者会话才能获得新的声明
			if err := client.Consume(ctx, strings.Split(topics, ","), &consumer); err != nil {
				utils.Logger.Errorf("Error from consumer: %v", err)
			}
			// 检查上下文是否被取消，表示使用者应该停止
			if ctx.Err() != nil {
				break
			}
			consumer.ready = make(chan bool)
		}
	}()

	<-consumer.ready //等待消费者设置完毕
	cancel()
	wg.Wait()

	if err := client.Close(); err != nil {
		utils.Logger.Errorf("Error closing client: %v", err)
	}
}

// Consumer 表示Sarama消费者组的消费者
type Consumer struct {
	ready chan bool
}

// Setup 在新会话开始时运行，在ConsumeClaim之前
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// 将消费者标记为就绪
	close(consumer.ready)
	return nil
}

// Cleanup 在会话结束时，一旦所有ConsumerClaim goroutine退出，就运行
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim 必须循环启动ConsumerGroupClaim消息的使用者
func (consumer *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	//注意：
	//不要将下面的代码移动到goroutine。
	//“consumerclaim”本身在goroutine中调用，请参见：
	// https://github.com/Shopify/sarama/blob/master/consumer_group.go#L27-L29

	for message := range claim.Messages() {
		// 准备发送数据给es
		msg := model.LogData{
			Topic: message.Topic,
			Data:  string(message.Value),
		}
		es.SaveToChan(&msg)
		//将消息标记为已使用
		session.MarkMessage(message, "")
	}
	return nil
}
