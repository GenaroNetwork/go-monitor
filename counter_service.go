package main

import (
	"context"
	"database/sql"
	"errors"
	"math/big"
	"sync"

	"github.com/gomodule/redigo/redis"
)

const MaxBlockRangeSpan = 10000

var _counterService *CounterService = nil

type BlockRange struct {
	from *big.Int
	to   *big.Int
}

/********************/

type Counter struct {
	headNum  *big.Int
	cRange   chan *BlockRange
	redisCon redis.Conn
}

// generate BlockRange continuously, at the meanwhile allow the `newHeadNum` to be updated
func (c *Counter) start(ctx context.Context, cHeadNum chan *big.Int) {
	mu := sync.Mutex{}
	updateEvent := make(chan struct{})
	var newHeadNum *big.Int
	go func() {
		for {
			select {
			case headNum := <-cHeadNum:
				mu.Lock()
				if newHeadNum == nil {
					newHeadNum = headNum
				} else if headNum.Cmp(newHeadNum) > 0 {
					newHeadNum = headNum
				}
				mu.Unlock()
				select {
				case updateEvent <- struct{}{}:
				default:
				}
			case <-ctx.Done():
				break
			}
		}
	}()
	go func() {
	loop:
		for {
			mu.Lock()
			if newHeadNum == nil || newHeadNum.Cmp(c.headNum) <= 0 {
				mu.Unlock()
				select {
				case <-updateEvent:
					continue
				case <-ctx.Done():
					break loop
				}
			}
			num := new(big.Int).Add(c.headNum, big.NewInt(MaxBlockRangeSpan))
			if num.Cmp(newHeadNum) > 0 {
				num = newHeadNum
			}
			mu.Unlock()
			br := &BlockRange{from: c.headNum, to: num}
			select {
			case c.cRange <- br:
			case <-ctx.Done():
				break loop
			}
			c.headNum = num
		}
	}()
}

/********************/

type CounterService struct {
	counter *Counter
	mu      *sync.Mutex
}

func NewCounterService() *CounterService {
	if _counterService != nil {
		return _counterService
	}
	_counterService = &CounterService{counter: nil, mu: &sync.Mutex{}}
	return _counterService
}

// Lazily create a Counter in singleton pattern
func (c *CounterService) Counter(ctx context.Context, pgCon *sql.DB, cHeadNum chan *big.Int) (*Counter, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.counter != nil {
		return c.counter, nil
	}
	rows, err := pgCon.Query("select head_num from settings where id=1")
	if err != nil {
		return nil, err
	}

	var headNum *big.Int
	if rows.Next() {
		var headNumStr string
		err = rows.Scan(&headNumStr)
		if err != nil {
			return nil, err
		}

		var ok bool
		headNum, ok = new(big.Int).SetString(headNumStr, 10)

		if ok == false {
			return nil, errors.New("parsed db field head_num failed")
		}
	} else {
		headNum = big.NewInt(0)
	}

	counter := Counter{
		headNum: headNum,
		cRange:  make(chan *BlockRange),
	}
	counter.start(ctx, cHeadNum)
	return &counter, nil
}
