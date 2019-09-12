package main

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

func Subscribe(rawUrl string, out chan *big.Int) error {
	// create rpc client
	client, err := rpc.Dial(rawUrl)
	if err != nil {
		return err
	}

	// subscribe to new block notification
	// reconnect when lost connection
	subChan := make(chan *types.Header)
	go func() {
		for i := 0; ; i++ {
			if i > 0 {
				time.Sleep(2 * time.Second)
			}
			fmt.Println("connecting to server")
			subscribeBlocks(client, subChan)
		}
	}()
	go func() {
		for block := range subChan {
			out <- block.Number
		}
	}()
	return nil
}

func subscribeBlocks(client *rpc.Client, subChan chan *types.Header) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sub, err := client.EthSubscribe(ctx, subChan, "newHeads")
	if err != nil {
		fmt.Println("subscribe error:", err)
		return
	}
	fmt.Println("connection lost:", <-sub.Err())
}
