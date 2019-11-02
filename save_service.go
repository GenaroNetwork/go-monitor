package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/big"
	"time"
)

type SaveService struct {
	ChanIn chan *Task
	pgCon  *sql.DB
}

func NewSaveService(pgCon *sql.DB, cache uint) SaveService {
	return SaveService{
		ChanIn: make(chan *Task, cache),
		pgCon:  pgCon,
	}
}

func (s *SaveService) Start(ctx context.Context) {
	for {
		task := <-s.ChanIn
		queryData := task.queryData
		if queryData.IsEmpty() {
			task.DoneSave()
			continue
		}
		for {
			// TODO: fix context
			ok := s.Save(ctx, task.blockRange.to, queryData)
			if ok {
				task.DoneSave()
				fmt.Printf("done %v %v\n", task.blockRange.from, task.blockRange.to)
				break
			} else {
				fmt.Println("save to database failed")
				time.Sleep(time.Millisecond * 100)
			}
		}
	}
}

func (s *SaveService) Save(ctx context.Context, headNum *big.Int, queryData *QueryData) bool {
	var tx *sql.Tx
	var err error
	var rows *sql.Rows

	tx, err = s.pgCon.BeginTx(ctx, nil)
	if err != nil {
		fmt.Printf("sql.BeginTx err: %v\n", err)
		return false
	}

	var headNumStmt *sql.Stmt
	headNumStmt, err = tx.Prepare("insert into settings (id, head_num) values (1, $1) on conflict(id) do update set head_num=$1")
	if err != nil {
		fmt.Printf("sql.Prepare headNumStmt err: %v\n", err)
		return false
	}
	defer headNumStmt.Close()
	rows, err = headNumStmt.Query(headNum.String())
	if err != nil {
		fmt.Printf("update head_num failed %v err=%v\n", headNum, err)
		return false
	}
	if rows.Next() {
		var num string
		err = rows.Scan(&num)
		fmt.Printf("upsert head_num num=%v err=%v\n", num, err)
	}
	rows.Close()

	var trafficStmt *sql.Stmt
	trafficStmt, err = tx.Prepare("insert into users (id) values ($1) on conflict (id) do update set traffic=$2")
	if err != nil {
		fmt.Printf("sql.Prepare trafficStmt err: %v\n", err)
		err = tx.Rollback()
		if err != nil {
			panicErr(err)
		}
		return false
	}
	defer trafficStmt.Close()

	for _, trafficTxInfo := range queryData.TrafficTxInfoList {
		rows, err = trafficStmt.Query(trafficTxInfo.Address, trafficTxInfo.Traffic)
		if err != nil {
			fmt.Printf("upsert users.traffic failed address=%v traffic=%v err=%v\n", trafficTxInfo.Address, trafficTxInfo.Traffic, err)
			err = tx.Rollback()
			if err != nil {
				panicErr(err)
			}
			return false
		}
		rows.Close()
	}

	var bucketStmt *sql.Stmt
	bucketStmt, err = tx.Prepare("insert into buckets (id, uid, size, time_start, time_end, backup, name) VALUES ($1, $2, $3, $4, $5, $6, $7) on conflict (id) do update set (time_end, size) = (excluded.time_end, excluded.size)")
	if err != nil {
		fmt.Printf("sql.Prepare bucketStmt err: %v\n", err)
		err = tx.Rollback()
		if err != nil {
			panicErr(err)
		}
		return false
	}
	defer bucketStmt.Close()

	for _, bucketTxInfo := range queryData.BucketTxInfoList {
		rows, err = bucketStmt.Query(
			bucketTxInfo.BucketId,
			bucketTxInfo.Address,
			bucketTxInfo.Size,
			bucketTxInfo.TimeStart,
			bucketTxInfo.TimeEnd,
			bucketTxInfo.Backup,
			"New Bucket",
		)
		if err != nil {
			fmt.Printf("insert buckets failed bucketTxInfo=%v err=%v\n", bucketTxInfo, err)
			err = tx.Rollback()
			if err != nil {
				panicErr(err)
			}
			return false
		}
		rows.Close()
	}

	var suppStmt *sql.Stmt
	suppStmt, err = tx.Prepare("update buckets set (time_end, size) = (time_end + $1, size + $2) where id=$3")
	if err != nil {
		fmt.Printf("sql.Prepare suppStmt err: %v\n", err)
		err = tx.Rollback()
		if err != nil {
			panicErr(err)
		}
		return false
	}
	defer suppStmt.Close()

	for _, suppTxInfo := range queryData.SuppTxInfoList {
		rows, err = suppStmt.Query(suppTxInfo.Duration, suppTxInfo.Size, suppTxInfo.BucketId)
		if err != nil {
			fmt.Printf("update buckets failed suppTxInfo=%v\n", suppTxInfo)
			err = tx.Rollback()
			if err != nil {
				panicErr(err)
			}
			return false
		}
		rows.Close()
	}

	err = tx.Commit()
	if err != nil {
		panicErr(err)
	}
	return true
}
