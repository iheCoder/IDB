package IDB

import (
	"errors"
	"sync"
)

var (
	ErrNoSuchRecordInUndoInspector = errors.New("inspector: no such record")
)

// 这可是必要组件啊，如果没有这个事务无法支撑。难道这不应该必有嘛。可对于bptree来说却并非如此
type UndoInspector struct {
	data  *sharedData
	delMu *sync.Mutex
	upMu  *sync.Mutex
}

type sharedData struct {
	upData  map[int]*txUpdateData
	delData map[int]*Record
}

type txUpdateData struct {
	txRecords map[int]*txUpdateRecord
}

type txUpdateRecord struct {
	record *Record
}

func (ui *UndoInspector) HandleDataBeforeUpdate(oldRecord *Record, newRecordMeta *RecordMeta) {
	if newRecordMeta == nil {
		return
	}
	ui.upMu.Lock()
	defer ui.upMu.Unlock()

	values := make([]string, len(oldRecord.Value))
	copy(values, oldRecord.Value)
	if ui.data.upData[newRecordMeta.LastTxID] == nil {
		ui.data.upData[newRecordMeta.LastTxID] = &txUpdateData{txRecords: make(map[int]*txUpdateRecord)}
	}
	txUpData := ui.data.upData[newRecordMeta.LastTxID]
	txUpData.txRecords[oldRecord.Key] = &txUpdateRecord{
		record: &Record{
			Key:   oldRecord.Key,
			Value: values,
		},
	}
}

func (ui *UndoInspector) HandleDataBeforeDelete(record *Record) {
	ui.delMu.Lock()
	defer ui.delMu.Unlock()

	values := make([]string, len(record.Value))
	copy(values, record.Value)
	ui.data.delData[record.Key] = &Record{
		Key:   record.Key,
		Value: values,
	}
}

func (ui *UndoInspector) GetRecordBeforeUpdate(txID, recordID int) (*Record, error) {
	ui.upMu.Lock()
	defer ui.upMu.Unlock()

	txData := ui.data.upData[txID]
	if txData == nil {
		return nil, ErrNoSuchRecordInUndoInspector
	}

	record := txData.txRecords[recordID]
	if record == nil {
		return nil, ErrNoSuchRecordInUndoInspector
	}
	delete(txData.txRecords, recordID)
	if len(txData.txRecords) == 0 {
		delete(ui.data.upData, txID)
	}
	return record.record, nil
}

func (ui *UndoInspector) GetRecordBeforeDelete(recordID int) (*Record, error) {
	ui.delMu.Lock()
	defer ui.delMu.Unlock()

	record := ui.data.delData[recordID]
	if record == nil {
		return nil, ErrNoSuchRecordInUndoInspector
	}
	delete(ui.data.delData, recordID)
	return record, nil
}

func NewUndoInspector() *UndoInspector {
	return &UndoInspector{
		data: &sharedData{
			upData:  make(map[int]*txUpdateData),
			delData: make(map[int]*Record),
		},
		delMu: &sync.Mutex{},
		upMu:  &sync.Mutex{},
	}
}
