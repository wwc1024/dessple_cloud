package common

import "github.com/asim/go-micro/v3/config"

//{
//	"host":"127.0.0.1",
//  "user":"root",
//  "pwd":"123456",
//  "database":"pass",
//  "port":"3306"
//}
//创建结构体

type MysqlConfig struct {
	Host     string `json:"host"`
	User     string `json:"user"`
	Pwd      string `json:"pwd"`
	Database string `json:"database"`
	Port     string `json:"port"`
}

func GetMysqlFromConsul(config config.Config, path ...string) *MysqlConfig {
	mysqlConfig := &MysqlConfig{}
	config.Get(path...).Scan(mysqlConfig)
	return mysqlConfig
}
