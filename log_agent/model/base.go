package model

//LogConf 需要收集的日志信息
type LogConf struct {
	Path  string //被收集的日志存放的路径
	Topic string //将日志发送到kafka对应的topic
}

//LogData 日志信息
type LogData struct {
	Data  string //日志信息
	Topic string //将日志发送到kafka对应的topic
}
