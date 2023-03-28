package IDB

import (
	"errors"
)

var (
	ErrRecordNotCommit = errors.New("undo_log record not commit")
)

type UndoLog struct {
	items     []*UndoLogItem
	idCounter int
	// 记录id、该记录所有commit record
	recordsCache map[int]*undoRecord
	// 事务tx所有可能被影响的logItem
	// TODO 总感觉这个怪怪的
	affectTxItems map[int][]*UndoLogItem
}

type undoRecord struct {
	records     []*UndoRecord
	id          []int
	commitTxIDs []int
}

type UndoLogItem struct {
	// TODO 显然根据commitTxID去排序是一个错误！！！应该按时间去添加
	commitTxID       int
	records          map[int]*UndoRecord
	mayAffectedTxIDs map[int]bool
	id               int
}

type UndoRecord struct {
	record *Record
	op     opType
}

// Append 在提交事务之后添加undoLog
// LastTxID 该事务ID
// mayAffectedTxIDs 可能被影响事务IDs
// records 该提交的undo操作
func (l *UndoLog) Append(txID int, mayAffectedTxIDs map[int]bool, undoRecords []*UndoRecord) {
	// 从所有影响txIDs中删除，若对应缓存影响txIDs为空，则删除该undoLog
	logItems, ok := l.affectTxItems[txID]
	if ok {
		for _, logItem := range logItems {
			delete(logItem.mayAffectedTxIDs, txID)
			if len(logItem.mayAffectedTxIDs) == 0 {
				// 若连续删除就会导致原来的索引位置不对
				l.removeItem(logItem)
			}
		}
		delete(l.affectTxItems, txID)
	}

	// 若可能影响的事务为空，那就不必记录
	if len(mayAffectedTxIDs) == 0 {
		return
	}

	item := &UndoLogItem{
		commitTxID:       txID,
		records:          convRecordCacheSliceToMap(undoRecords),
		mayAffectedTxIDs: mayAffectedTxIDs,
	}
	// 添加当前item到所有未来可能影响的事务
	for tid, _ := range mayAffectedTxIDs {
		_, ok = l.affectTxItems[tid]
		if !ok {
			l.affectTxItems[tid] = []*UndoLogItem{
				item,
			}
		} else {
			l.affectTxItems[tid] = append(l.affectTxItems[tid], item)
		}
	}

	// 新增logItem
	l.items = append(l.items, item)
	id := l.idCounter
	l.idCounter++
	item.id = id

	// 缓存recordID以及所有更新过该record的历史记录
	for recordIndex, recordCache := range item.records {
		rc, ok := l.recordsCache[recordIndex]
		if !ok {
			l.recordsCache[recordIndex] = &undoRecord{
				records:     []*UndoRecord{recordCache},
				id:          []int{id},
				commitTxIDs: []int{txID},
			}
		} else {
			rc.records = append(rc.records, recordCache)
			rc.id = append(rc.id, id)
			rc.commitTxIDs = append(rc.commitTxIDs, txID)
		}
	}
}

func (l *UndoLog) removeItem(item *UndoLogItem) {
	// TODO 如何删除缓存里面的东西呢？显然要联系undoLogItem里面的recordCache以及undoRecord里面的recordCache
	for rid, _ := range item.records {
		rc := l.recordsCache[rid]
		if rc != nil {
			for i := 0; i < len(rc.id); i++ {
				// recordCache记录的是index，可是一旦去删除的话，index是会改变的
				if rc.id[i] == item.id {
					rc.id = append(rc.id[:i], rc.id[i+1:]...)
					rc.records = append(rc.records[:i], rc.records[i+1:]...)
					rc.commitTxIDs = append(rc.commitTxIDs[:i], rc.commitTxIDs[i+1:]...)
					i--
				}
			}

			if len(rc.id) == 0 {
				delete(l.recordsCache, rid)
			}
		}
	}

	var i int
	for i = 0; i < len(l.items); i++ {
		if item.id == l.items[i].id {
			break
		}
	}
	l.items = append(l.items[:i], l.items[i+1:]...)
}

// TODO 问题在于如何找到最晚提交的那个活跃txID？
func (l *UndoLog) Find(activeTxIDs map[int]bool, recordID int) (*Record, error) {
	// 尝试从缓存中找该记录
	rcs, ok := l.recordsCache[recordID]
	if !ok {
		return nil, ErrRecordNotCommit
	}

	// 尝试找到最早更新过该record的活跃txID
	for i, txID := range rcs.commitTxIDs {
		if activeTxIDs[txID] {
			return transferToRecord(rcs.records[i])
		}
	}

	// 若没有任何事务更新过该record，则返回错误
	return nil, ErrRecordNotCommit
}

func (l *UndoLog) IsEmpty() bool {
	return len(l.items) == 0
}

func transferToRecord(rc *UndoRecord) (*Record, error) {
	switch rc.op {
	case DELETE:
		return nil, nil
	default:
		return rc.record, nil
	}
}

func convRecordCacheSliceToMap(records []*UndoRecord) map[int]*UndoRecord {
	rcm := make(map[int]*UndoRecord, len(records))
	for _, record := range records {
		rcm[record.record.Key] = record
	}
	return rcm
}

func NewUndoLog() *UndoLog {
	return &UndoLog{
		items:         make([]*UndoLogItem, 0),
		recordsCache:  make(map[int]*undoRecord),
		affectTxItems: make(map[int][]*UndoLogItem),
		idCounter:     0,
	}
}
