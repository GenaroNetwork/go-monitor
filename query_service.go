package main

import (
	"context"
	"fmt"
)

type QueryData struct {
	TrafficTxInfoList []TrafficTxInfo
	BucketTxInfoList  []BucketTxInfo
	SuppTxInfoList    []SuppTxInfo
}

func (q *QueryData) IsEmpty() bool {
	return (len(q.TrafficTxInfoList) + len(q.BucketTxInfoList) + len(q.SuppTxInfoList)) == 0
}

type QueryWorker struct {
	ChanIn chan *Task
	Done   chan *QueryWorker
}

func NewQueryWorker(ch chan *QueryWorker) QueryWorker {
	return QueryWorker{
		ChanIn: make(chan *Task),
		Done:   ch,
	}
}

func (w *QueryWorker) Run(ctx context.Context) {
	for task := range w.ChanIn {
		for {
			fmt.Printf("query worker range: %v %v\n", task.blockRange.from, task.blockRange.to)
			err := w.queryInRange(ctx, task)
			if err == nil {
				break
			}
			if err == context.Canceled {
				return
			}
		}
	}
}

func (w *QueryWorker) queryInRange(ctx context.Context, task *Task) error {
	client, err := NewQueryClient(WsServer)
	if err != nil {
		fmt.Println("client e", err)
		return err
	}

	queryData := QueryData{}
	from := task.blockRange.from
	to := task.blockRange.to

	// trafficTxInfo
	trafficTxInfoList, err := client.QueryTrafficTxInfo(ctx, from, to)
	if err != nil {
		fmt.Printf("QueryTrafficTxInfo err: %v\n", err)
		return err
	}
	queryData.TrafficTxInfoList = trafficTxInfoList

	// bucketTxInfo
	bucketTxInfoList, err := client.QueryBucketTxInfo(ctx, from, to)
	if err != nil {
		fmt.Printf("QueryBucketTxInfo err: %v\n", err)
		return err
	}
	queryData.BucketTxInfoList = bucketTxInfoList

	// suppTxInfo
	suppTxInfoList, err := client.QuerySuppTxInfo(ctx, from, to)
	if err != nil {
		fmt.Printf("QuerySuppTxInfo err: %v\n", err)
		return err
	}
	queryData.SuppTxInfoList = suppTxInfoList

	fmt.Printf("queried %v %v\n", task.blockRange.from, task.blockRange.to)
	task.queryData = &queryData
	task.DoneQuery()
	w.Done <- w
	return nil
}

/*****************************/

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
