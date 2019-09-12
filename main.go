package main

import (
	"database/sql"
	"fmt"
	"math/big"

	"github.com/gomodule/redigo/redis"
	_ "github.com/lib/pq"
	"golang.org/x/net/context"
)

const (
	DBHost     = "localhost"
	DBPort     = 5432
	DBUser     = "genaro"
	DBPassword = "null"
	DBName     = "bridge"
)

const (
	WsServer = "ws://47.100.34.71:8547"
)
const (
	RedisKeyFromNum = "m:b"
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

	// context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// subscribe for new block
	chanBlockNumber := make(chan *big.Int)
	Subscribe(WsServer, chanBlockNumber)

	// prepare Counter
	counterService := NewCounterService()
	counter, err := counterService.Counter(ctx, redisCon, chanBlockNumber)
	panicErr(err)

	// prepare QueryClient
	client, err := NewQueryClient(WsServer)
	panicErr(err)

	// start query service
	ctxQueryClient := context.WithValue(ctx, "client", client)
	queryService := NewQueryService(1)
	go queryService.Start(ctxQueryClient)

	// start save service
	saveService := NewSaveService(100)
	go saveService.Start(ctx)

	for br := range counter.ChanOut {
		queryWorker := <-queryService.Workers
		queryWorker.ChanIn <- br
		saveService.ChanIn <- queryWorker.ChanOut
	}

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
