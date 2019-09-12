package main

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethereum/go-ethereum/rpc"
)

type QueryClient struct {
	*rpc.Client
}

func NewQueryClient(rawUrl string) (*QueryClient, error) {
	// create rpc client
	client, err := rpc.Dial(rawUrl)
	if err != nil {
		return nil, err
	}
	return &QueryClient{client}, nil
}

/******************************/

type TrafficTxInfo struct {
	// user address
	Address string `json:"address"`
	// GB
	Traffic int `json:"traffic"`
}

func (c *QueryClient) QueryTrafficTxInfo(ctx context.Context, from *big.Int, to *big.Int) ([]TrafficTxInfo, error) {
	var result []TrafficTxInfo
	err := c.CallContext(ctx, &result, "eth_getBucketTxInfo", hex(from), hex(to))
	return result, err
}

/******************************/

type BucketTxInfo struct {
	Address   string `json:"address"`
	BucketId  string `json:"bucketId"`
	TimeStart uint64 `json:"timeStart"`
	TimeEnd   uint64 `json:"timeEnd"`
	Backup    uint64 `json:"backup"`
	Size      uint64 `json:"size"`
}

func (c *QueryClient) QueryBucketTxInfo(ctx context.Context, from *big.Int, to *big.Int) ([]BucketTxInfo, error) {
	var result []BucketTxInfo
	err := c.CallContext(ctx, &result, "eth_getBucketTxInfo", hex(from), hex(to))
	return result, err
}

/******************************/

type SuppTxInfo struct {
	BucketId string `json:"bucketId"`
	Address  string `json:"address"`
	Size     uint64 `json:"size"`
	Duration uint64 `json:"duration"`
}

func (c *QueryClient) QuerySuppTxInfo(ctx context.Context, from *big.Int, to *big.Int) ([]SuppTxInfo, error) {
	var result []SuppTxInfo
	err := c.CallContext(ctx, &result, "eth_getBucketSupplementTx", hex(from), hex(to))
	return result, err
}

/********************************/

type ShareTxInfo struct {
	ShareKey   string `json:"shareKey"`
	ShareKeyId string `json:"shareKeyId"`
	MailHash   string `json:"mail_hash"`
	MailSize   uint64 `json:"mail_size"`
}

func (c *QueryClient) QueryShareTxInfo(ctx context.Context, from *big.Int, to *big.Int) (*[]ShareTxInfo, error) {
	var result []ShareTxInfo
	err := c.CallContext(ctx, &result, "eth_getSynchronizeShareKey", hex(from), hex(to))
	return &result, err
}

/********************************/

func hex(i *big.Int) string {
	return fmt.Sprintf("0x%x", i)
}
