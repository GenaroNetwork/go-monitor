package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"os"
	"os/signal"

	"github.com/gomodule/redigo/redis"
	_ "github.com/lib/pq"
)

const (
	DBHost     = "localhost"
	DBPort     = 5432
	DBUser     = "genaro"
	DBPassword = "null"
	DBName     = "bridge"
)

const (
	WsServer = "ws://101.132.159.197:8547"
)
const (
	RedisKeyFromNum = "m:b" // monitor:block
)

var redisCon redis.Conn
var pgCon *sql.DB

func main() {
	var err error
	// connect to db
	dbInfo := fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
		DBHost, DBPort, DBUser, DBPassword, DBName)
	pgCon, err = sql.Open("postgres", dbInfo)
	panicErr(err)

	// test connection
	err = pgCon.Ping()
	panicErr(err)

	// connect to redis
	redisCon, err = redis.Dial("tcp", "localhost:6379")
	panicErr(err)
	defer redisCon.Close()

	// subscribe for new block
	chanBlockNumber := make(chan *big.Int)
	err = Subscribe(WsServer, chanBlockNumber)
	panicErr(err)

	// context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskManager := NewTaskManager()
	err = taskManager.Run(ctx, chanBlockNumber)
	panicErr(err)

	exit := make(chan os.Signal)
	signal.Notify(exit, os.Interrupt, os.Kill)
	<-exit

	// shareTxInfo
	//shareTxInfo, err := client.QueryShareTxInfo(big.NewInt(200000), big.NewInt(286400))
	//if err == nil {
	//	fmt.Printf("shareTxInfo: %v\n", shareTxInfo)
	//} else {
	//	fmt.Printf("err: %v\n", err)
	//}
}

func panicErr(err error) {
	if err != nil {
		panic(err)
	}
}
