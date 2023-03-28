package IDB

import (
	"errors"
	"math"
	"sync"
	"sync/atomic"
)

const (
	InvalidTxID = math.MaxInt
)

var (
	ErrNoSuchATableInTxMgr = errors.New("transaction: no such table in tx mgr")
)

type TxMgr interface {
	StartTransaction() *Tx
	AfterCommit(tx *Tx)
	AfterRollback(tx *Tx)
	FindRecordInUndoLog(tableName string, recordID int, activeTxIDs map[int]bool) (*Record, error)
}

type TxExecutor interface {
	commit(cache map[string]*txCache) error
}

type UndoRecordsCollector interface {
	GetRecordBeforeUpdate(txID, recordID int) (*Record, error)
	GetRecordBeforeDelete(recordID int) (*Record, error)
}

type Tx struct {
	id       int
	executor TxExecutor
	cache    map[string]*txCache
	rv       *readView
	mgr      TxMgr
}

type readView struct {
	activeTxIDs map[int]bool
	minActiveID int
	nextTxID    int
}

type txCache struct {
	t *table
	// recordID以及该record的操作记录
	cache map[int]*OpRecord
}

type opType int

const (
	INVALID opType = iota
	UPDATE
	INSERT
	DELETE
)

type OpRecord struct {
	op       opType
	opChange interface{}
	LastTxID int
}

type DeleteOpChange struct {
	id int
}

type InsertOpChange struct {
	record *Record
}

type UpdateOpChange struct {
	change map[int]string
	// 该record保证可重复读。记录第一次查询到的record以及之后更新的字段
	record *Record
}

func (tx *Tx) Commit() error {
	err := tx.executor.commit(tx.cache)
	if err != nil {
		return tx.Rollback()
	}
	tx.mgr.AfterCommit(tx)
	return nil
}

func (tx *Tx) Rollback() error {
	defer tx.mgr.AfterRollback(tx)
	tx.cache = make(map[string]*txCache)
	return nil
}

type TxMgrImpl struct {
	txIDCounter int64
	executor    TxExecutor
	// tx readView修改需要锁
	mu            *sync.RWMutex
	activeTxIDs   map[int]bool
	undoLogs      map[string]*UndoLog
	undoCollector UndoRecordsCollector
}

func (tm *TxMgrImpl) StartTransaction() *Tx {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	id := int(atomic.AddInt64(&(tm.txIDCounter), 1))
	minTxID := InvalidTxID
	for txid, _ := range tm.activeTxIDs {
		minTxID = minInt(minTxID, txid)
	}
	rv := &readView{
		// TODO map不复制就会跟着原来的map一直变化
		activeTxIDs: copyMap(tm.activeTxIDs),
		minActiveID: minTxID,
		nextTxID:    id + 1,
	}

	tm.activeTxIDs[id] = true

	return &Tx{
		id:       id,
		executor: tm.executor,
		cache:    make(map[string]*txCache),
		rv:       rv,
		mgr:      tm,
	}
}

func (tm *TxMgrImpl) DeactivateTx(tx *Tx) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	delete(tm.activeTxIDs, tx.id)
}

func (tm *TxMgrImpl) AfterCommit(tx *Tx) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	delete(tm.activeTxIDs, tx.id)

	// 遍历cache将提交添加到undoLog中
	for tableName, tc := range tx.cache {
		if tm.undoLogs[tableName] == nil {
			tm.undoLogs[tableName] = NewUndoLog()
		}
		log := tm.undoLogs[tableName]
		log.Append(tx.id, tm.activeTxIDs, tm.convToUndoRecord(tx.id, tc.cache))
	}
}

func (tm *TxMgrImpl) convToUndoRecord(txID int, rcs map[int]*OpRecord) []*UndoRecord {
	// TODO 更新、删除的时候这不还要求传递原来record的进来嘛
	records := make([]*UndoRecord, 0, len(rcs))
	for rid, opRecord := range rcs {
		switch opRecord.op {
		case UPDATE:
			r, err := tm.undoCollector.GetRecordBeforeUpdate(txID, rid)
			if err != nil {
				if err == ErrNoSuchRecordInUndoInspector {
					continue
				}
				panic(err)
			}
			ur := &UndoRecord{
				record: r,
				op:     UPDATE,
			}
			records = append(records, ur)

		case DELETE:
			r, err := tm.undoCollector.GetRecordBeforeDelete(rid)
			if err != nil {
				if err == ErrNoSuchRecordInUndoInspector {
					continue
				}
				panic(err)
			}
			ur := &UndoRecord{
				record: r,
				op:     INSERT,
			}
			records = append(records, ur)

		case INSERT:
			ur := &UndoRecord{
				record: &Record{Key: rid},
				op:     DELETE,
			}
			records = append(records, ur)

		default:
			panic("unexpected op")
		}
	}

	return records
}

func (tm *TxMgrImpl) AfterRollback(tx *Tx) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	delete(tm.activeTxIDs, tx.id)

	// 删除被该事务影响的缓存
	for tableName, _ := range tx.cache {
		// 若为nil，就别管了
		if tm.undoLogs[tableName] == nil {
			continue
		}
		// 若undoLog为空也别管了
		if tm.undoLogs[tableName].IsEmpty() {
			delete(tm.undoLogs, tableName)
			continue
		}
		// 更新一下这个事务回滚，然后让log去删除
		log := tm.undoLogs[tableName]
		log.Append(tx.id, nil, nil)
	}
}

func (tm *TxMgrImpl) FindRecordInUndoLog(tableName string, recordID int, activeTxIDs map[int]bool) (*Record, error) {
	if tm.undoLogs[tableName] == nil {
		return nil, ErrNoSuchATableInTxMgr
	}

	// 不应该用当前tx，而是start之后的tx
	return tm.undoLogs[tableName].Find(activeTxIDs, recordID)
}

func NewTxMgr(e TxExecutor, collector UndoRecordsCollector) TxMgr {
	return &TxMgrImpl{
		txIDCounter:   0,
		executor:      e,
		mu:            &sync.RWMutex{},
		activeTxIDs:   make(map[int]bool),
		undoLogs:      make(map[string]*UndoLog),
		undoCollector: collector,
	}
}
