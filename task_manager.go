package main

import (
	"context"
	"fmt"
	"math/big"
	"sync"
)

type TaskManager struct {
	mu    sync.Mutex
	tasks []*Task
}

func NewTaskManager() TaskManager {
	return TaskManager{}
}

func (t *TaskManager) Run(ctx context.Context, chanBlockNumber chan *big.Int) error {
	// prepare Counter
	counterService := NewCounterService()
	counter, err := counterService.Counter(ctx, redisCon, chanBlockNumber)
	if err != nil {
		return err
	}

	// prepare QueryClient
	_, err = NewQueryClient(WsServer)
	if err != nil {
		return err
	}

	// start query service
	queryService := NewQueryService(3)
	go queryService.Start(ctx)

	// start save service
	saveService := NewSaveService(1)
	go saveService.Start(ctx)

	eventNonEmpty := make(chan struct{})
	// create and dispatch tasks, to QueryService
	// the tasks will be executed immediately in parallel,
	// while the order be preserved in TaskManager.tasks
	go func() {
	quit:
		for {
			select {
			case blockRange := <-counter.ChanOut:
				task := &Task{
					blockRange: blockRange,
					doneQuery:  make(chan struct{}),
					doneSave:   make(chan struct{}),
				}
				t.mu.Lock()
				if len(t.tasks) == 0 {
					select {
					case eventNonEmpty <- struct{}{}:
					case <-ctx.Done():
						fmt.Println("canceled while notifying the first task")
						break quit
					}
				}
				t.tasks = append(t.tasks, task)
				t.mu.Unlock()
				select {
				case worker := <-queryService.Workers:
					select {
					case worker.ChanIn <- task:
					case <-ctx.Done():
						fmt.Println("canceled while dispatch task")
						break quit
					}
				case <-ctx.Done():
					fmt.Println("canceled while getting QueryWorker")
					break quit
				}
			case <-ctx.Done():
				fmt.Println("canceled while waiting for BlockRange")
				break quit
			}
		}
	}()
	// waiting for the tasks to be done,
	// and save the results in the order they created
	go func() {
	quit:
		for {
			t.mu.Lock()
			if len(t.tasks) != 0 {
				t.mu.Unlock()
				task := t.tasks[0]
				select {
				case <-task.doneQuery:
					saveService.ChanIn <- task
					select {
					case <-task.doneSave:
						t.mu.Lock()
						t.tasks = t.tasks[1:]
						t.mu.Unlock()
						_, err := redisCon.Do("SET", RedisKeyFromNum, task.blockRange.to.String())
						if err != nil {
							fmt.Printf("save redis failed %v %v", task.blockRange.from, task.blockRange.to)
						}
					case <-ctx.Done():
						fmt.Println("canceled while waiting for doneSave")
						break quit
					}
				case <-ctx.Done():
					fmt.Println("canceled while waiting for doneQuery")
					break quit
				}
			} else {
				t.mu.Unlock()
				select {
				case <-eventNonEmpty:
				case <-ctx.Done():
					fmt.Println("canceled while waiting for first task")
					break quit
				}
			}
		}
	}()
	return nil
}

/*****************************/

type Task struct {
	blockRange *BlockRange
	queryData  *QueryData
	doneQuery  chan struct{}
	doneSave   chan struct{}
}

func (t *Task) DoneQuery() {
	close(t.doneQuery)
}

func (t *Task) DoneSave() {
	close(t.doneSave)
}
