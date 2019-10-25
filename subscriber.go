package main

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
)

func Subscribe(rawUrl string, cHeadNum chan *big.Int) error {
	// create rpc client
	client, err := rpc.Dial(rawUrl)
	if err != nil {
		return err
	}

	// subscribe to new block notification
	// reconnect when lost connection
	cHead := make(chan *types.Header)
	go func() {
		for i := 0; ; i++ {
			if i > 0 {
				time.Sleep(2 * time.Second)
			}
			fmt.Println("connecting to server")
			subHeads(client, cHead)
		}
	}()
	go func() {
		for head := range cHead {
			cHeadNum <- head.Number
		}
	}()
	return nil
}

func subHeads(client *rpc.Client, cHead chan *types.Header) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	sub, err := client.EthSubscribe(ctx, cHead, "newHeads")
	if err != nil {
		fmt.Println("subscribe error:", err)
		return
	}
	fmt.Println("connection lost:", <-sub.Err())
}
