package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"os"
	"os/signal"
	"time"

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

//const (
//	RedisKeyFromNum = "m:b" // monitor:block
//)

//var redisCon redis.Conn

func main() {
	var pgCon *sql.DB
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
	//redisCon, err = redis.Dial("tcp", "localhost:6379")
	//panicErr(err)
	//defer redisCon.Close()

	// subscribe for new block
	cHeadNum := make(chan *big.Int, 1)
	err = Subscribe(WsServer, cHeadNum)
	panicErr(err)

	// context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskManager := NewTaskManager()
	err = taskManager.Run(ctx, cHeadNum, pgCon)
	panicErr(err)

	exit := make(chan os.Signal)
	signal.Notify(exit, os.Interrupt, os.Kill)
	<-exit
	cancel()
	time.Sleep(time.Second * 5)

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
