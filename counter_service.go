package main

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"

	"github.com/gomodule/redigo/redis"
)

const MaxBlockRangeSpan = 10000

var _counterService *CounterService = nil

type BlockRange struct {
	from *big.Int
	to   *big.Int
	done chan struct{}
}

func (b *BlockRange) Done() {
	close(b.done)
}

/********************/

type Counter struct {
	fromNum  *big.Int
	ChanOut  chan *BlockRange
	redisCon redis.Conn

	muToNum sync.Mutex
	toNum   *big.Int
	updated chan struct{} // toNum updated, notify generator to generate new BlockRange

	muRange   sync.Mutex
	listRange []*BlockRange
	created   chan struct{}
}

// generate BlockRange continuously, at the meanwhile allow the `toNum` to be updated
func (c *Counter) start(ctx context.Context, chanToNum chan *big.Int) {
	go c.consumeToNum(ctx)
	go c.consumeBlockRange(ctx)
	go func() {
		for {
			select {
			case toNum := <-chanToNum:
				err := c.updateToNum(toNum)
				if err != nil {
					fmt.Printf("updateToNum error msg=%v\n", err)
				}
			case <-ctx.Done():
				break
			}
		}
	}()
}

func (c *Counter) updateToNum(toNum *big.Int) error {
	c.muToNum.Lock()
	defer c.muToNum.Unlock()
	// unexpected result, should be paid attention to,
	// but might not be an error in some rare corner situation
	// TODO: potential error
	if c.fromNum.Cmp(toNum) > 0 {
		return errors.New("Counter.fromNum > toNum")
	}
	if c.toNum != nil && c.toNum.Cmp(toNum) <= 0 {
		return errors.New("Counter.toNum <= toNum")
	}
	c.toNum = toNum
	fmt.Printf("updating toNum %v\n", toNum)
	c.updated <- struct{}{}
	fmt.Printf("updated toNum %v\n", toNum)
	return nil
}

// generate a series of BlockRange in between [fromNum, toNum],
// for each span of BlockRange, span <= MaxBlockRangeSpan <= 86400.
func (c *Counter) genBlockRange(toNum *big.Int) {
	end := false
	for {
		num := new(big.Int).Add(c.fromNum, big.NewInt(MaxBlockRangeSpan))
		if num.Cmp(toNum) > 0 {
			num = toNum
			end = true
		}
		mu := sync.Mutex{}
		mu.Lock()
		br := &BlockRange{from: c.fromNum, to: num, done: make(chan struct{})}
		c.ChanOut <- br
		c.fromNum = num

		c.muRange.Lock()
		c.listRange = append(c.listRange, br)
		c.muRange.Unlock()
		c.created <- struct{}{}
		if end {
			break
		}
	}
}

func (c *Counter) consumeToNum(ctx context.Context) {
	for {
		c.muToNum.Lock()
		if c.toNum != nil {
			toNum := c.toNum
			c.toNum = nil
			c.muToNum.Unlock()
			c.genBlockRange(toNum)
		} else {
			c.muToNum.Unlock()
			select {
			case <-c.updated:
			case <-ctx.Done():
				break
			}
		}
	}
}

func (c *Counter) consumeBlockRange(ctx context.Context) {
	var blockRange *BlockRange
quit:
	for {
		c.muRange.Lock()
		if blockRange != nil {
			c.muRange.Unlock()
			select {
			case <-blockRange.done:
				reply, err := c.redisCon.Do("SET", RedisKeyFromNum, blockRange.to.String())
				fmt.Printf("redis set %v %v %v\n", reply, err, blockRange.to)
				blockRange = nil
			case <-c.created:
			case <-ctx.Done():
				break quit
			}
		} else if len(c.listRange) != 0 {
			blockRange = c.listRange[0]
			c.listRange = c.listRange[1:]
			c.muRange.Unlock()
		} else {
			c.muRange.Unlock()
			select {
			case <-c.created:
			case <-ctx.Done():
				break quit
			}
		}
	}
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
func (c *CounterService) Counter(ctx context.Context, redisCon redis.Conn, chanToNum chan *big.Int) (*Counter, error) {
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

	var fromNum *big.Int
	if ok {
		fromNum, ok = new(big.Int).SetString(fromNumTxt, 10)
	}

	if ok == false {
		// TODO: event: redis data corrupted, restart from `0`
		fromNum = big.NewInt(0)
	}

	counter := Counter{
		fromNum:  fromNum,
		redisCon: redisCon,
		updated:  make(chan struct{}),
		created:  make(chan struct{}),
		ChanOut:  make(chan *BlockRange),
	}
	counter.start(ctx, chanToNum)
	return &counter, nil
}
