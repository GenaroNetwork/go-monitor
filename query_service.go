package main

import (
	"context"
	"fmt"
	"sync"
)

type QueryData struct {
	TrafficTxInfoList []TrafficTxInfo
	BucketTxInfoList  []BucketTxInfo
	SuppTxInfoList    []SuppTxInfo
	blockRange        *BlockRange
}

func (q *QueryData) IsEmpty() bool {
	return (len(q.TrafficTxInfoList) + len(q.BucketTxInfoList) + len(q.SuppTxInfoList)) == 0
}

type QueryWorker struct {
	ChanIn  chan *BlockRange
	ChanOut chan QueryData
	Done    chan *QueryWorker
}

func NewQueryWorker(ch chan *QueryWorker) QueryWorker {
	return QueryWorker{
		ChanIn:  make(chan *BlockRange),
		ChanOut: make(chan QueryData),
		Done:    ch,
	}
}

func (w *QueryWorker) Run(ctx context.Context) {
	for blockRange := range w.ChanIn {
		fmt.Printf("query worker range: %v %v\n", blockRange.from, blockRange.to)
		w.queryInRange(ctx, blockRange)
	}
}

func (w *QueryWorker) queryInRange(ctx context.Context, blockRange *BlockRange) {
	var wg sync.WaitGroup

	client, _ := ctx.Value("client").(*QueryClient)
	queryData := QueryData{blockRange: blockRange}
	from := blockRange.from
	to := blockRange.to
	// trafficTxInfo
	go func() {
		wg.Add(1)
		for {
			if ctx.Err() == context.Canceled {
				break
			}
			trafficTxInfoList, err := client.QueryTrafficTxInfo(ctx, from, to)
			if err != nil {
				fmt.Printf("QueryTrafficTxInfo err: %v\n", err)
				continue
			}
			queryData.TrafficTxInfoList = trafficTxInfoList
			break
		}
		wg.Done()

	}()

	// bucketTxInfo
	go func() {
		wg.Add(1)
		for {
			if ctx.Err() == context.Canceled {
				break
			}
			bucketTxInfoList, err := client.QueryBucketTxInfo(ctx, from, to)
			if err != nil {
				fmt.Printf("err: %v\n", err)
				continue
			}
			queryData.BucketTxInfoList = bucketTxInfoList
			break
		}
		wg.Done()
	}()

	// suppTxInfo
	go func() {
		wg.Add(1)
		for {
			if ctx.Err() == context.Canceled {
				break
			}
			suppTxInfoList, err := client.QuerySuppTxInfo(ctx, from, to)
			if err != nil {
				fmt.Printf("err: %v\n", err)
				continue
			}
			queryData.SuppTxInfoList = suppTxInfoList
			break
		}
		wg.Done()
	}()

	finished := make(chan struct{})
	go func() {
		wg.Wait()
		finished <- struct{}{}
	}()
	select {
	case <-ctx.Done():
	case <-finished:
		fmt.Printf("queried %v %v\n", queryData.blockRange.from, queryData.blockRange.to)
		w.ChanOut <- queryData
		w.Done <- w
	}
}

type QueryService struct {
	count   uint
	Workers chan *QueryWorker
}

func NewQueryService(count uint) QueryService {
	return QueryService{
		count:   count,
		Workers: make(chan *QueryWorker, count),
	}
}

func (s *QueryService) Start(ctx context.Context) {
	finished := make(chan *QueryWorker)
	for i := uint(0); i < s.count; i++ {
		worker := NewQueryWorker(finished)
		go worker.Run(ctx)
		s.Workers <- &worker
	}
	for {
		select {
		case s.Workers <- <-finished:
		case <-ctx.Done():
			break
		}
	}
}
