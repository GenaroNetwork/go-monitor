package main

import (
	"context"
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
func (c *CounterService) Counter(ctx context.Context, redisCon redis.Conn, cHeadNum chan *big.Int) (*Counter, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.counter != nil {
		return c.counter, nil
	}

	ok := true
	fromNumTxt, err := redis.String(redisCon.Do("GET", RedisKeyFromNum))
	if err == redis.ErrNil {
		ok = false
	} else if err != nil {
		return nil, err
	}

	var oldHeadNum *big.Int
	if ok {
		oldHeadNum, ok = new(big.Int).SetString(fromNumTxt, 10)
	}

	if ok == false {
		// TODO: event: redis data corrupted, restart from `0`
		oldHeadNum = big.NewInt(0)
	}

	counter := Counter{
		headNum:  oldHeadNum,
		redisCon: redisCon,
		cRange:   make(chan *BlockRange),
	}
	counter.start(ctx, cHeadNum)
	return &counter, nil
}
