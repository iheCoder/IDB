package IDB

import (
	"sync"
	"testing"
)

func TestCommit(t *testing.T) {
	server := NewIDBServer()
	fms := []*FieldMeta{
		{
			name:         "name",
			isPrimaryKey: false,
			tp:           STRING,
		},
	}

	tableName := "test"
	data := []interface{}{"hello"}

	server.CreateTable(tableName, fms)
	err := server.Insert(tableName, data)
	if err != nil {
		t.Fatal(err)
	}
	err = server.Insert(tableName, []interface{}{"world"})
	if err != nil {
		t.Fatal(err)
	}

	record, err := server.SelectByID(tableName, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 1 || record.Value[0] != "hello" {
		t.Fatal("mismatch result")
	}

	tm := NewTxMgr(server, nil)
	tx := tm.StartTransaction()
	upVals := map[string]interface{}{
		"name": "world",
	}
	err = server.UpdateByIDTx(tx, tableName, upVals, 1)
	if err != nil {
		return
	}

	record, err = server.SelectByID(tableName, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 1 || record.Value[0] != "hello" {
		t.Fatal("change data before commit")
	}

	tx.Commit()

	record, err = server.SelectByID(tableName, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 1 || record.Value[0] != "world" {
		t.Fatal("not change data after commit")
	}
}

func TestMultiUpdateSameInOneCommit(t *testing.T) {
	server := NewIDBServer()
	fms := []*FieldMeta{
		{
			name:         "name",
			isPrimaryKey: false,
			tp:           STRING,
		},
	}

	tableName := "test"
	data := []interface{}{"hello"}

	server.CreateTable(tableName, fms)
	err := server.Insert(tableName, data)
	if err != nil {
		t.Fatal(err)
	}
	err = server.Insert(tableName, []interface{}{"world"})
	if err != nil {
		t.Fatal(err)
	}

	record, err := server.SelectByID(tableName, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 1 || record.Value[0] != "hello" {
		t.Fatal("mismatch result")
	}

	tm := NewTxMgr(server, nil)
	tx := tm.StartTransaction()
	upVals := map[string]interface{}{
		"name": "world",
	}
	err = server.UpdateByIDTx(tx, tableName, upVals, 1)
	if err != nil {
		return
	}

	record, err = server.SelectByID(tableName, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 1 || record.Value[0] != "hello" {
		t.Fatal("change data before commit")
	}

	upVals = map[string]interface{}{
		"name": "python",
	}
	err = server.UpdateByIDTx(tx, tableName, upVals, 1)
	if err != nil {
		return
	}
	record, err = server.SelectByID(tableName, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 1 || record.Value[0] != "hello" {
		t.Fatal("change data before commit")
	}

	tx.Commit()
	record, err = server.SelectByID(tableName, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 1 || record.Value[0] != "python" {
		t.Fatal("change data before commit")
	}
}

func TestMulUpdateInOneCommit(t *testing.T) {
	server := NewIDBServer()
	fms := []*FieldMeta{
		{
			name:         "name",
			isPrimaryKey: false,
			tp:           STRING,
		},
	}

	tableName := "test"
	data := []interface{}{"hello"}

	server.CreateTable(tableName, fms)
	err := server.Insert(tableName, data)
	if err != nil {
		t.Fatal(err)
	}
	err = server.Insert(tableName, []interface{}{"world"})
	if err != nil {
		t.Fatal(err)
	}

	record, err := server.SelectByID(tableName, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 1 || record.Value[0] != "hello" {
		t.Fatal("mismatch result")
	}

	tm := NewTxMgr(server, nil)
	tx := tm.StartTransaction()
	upVals := map[string]interface{}{
		"name": "world",
	}
	err = server.UpdateByIDTx(tx, tableName, upVals, 1)
	if err != nil {
		return
	}

	record, err = server.SelectByID(tableName, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 1 || record.Value[0] != "hello" {
		t.Fatal("change data before commit")
	}

	upVals = map[string]interface{}{
		"name": "python",
	}
	err = server.UpdateByIDTx(tx, tableName, upVals, 1)
	if err != nil {
		return
	}
	record, err = server.SelectByID(tableName, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 1 || record.Value[0] != "hello" {
		t.Fatal("change data before commit")
	}

	upVals = map[string]interface{}{
		"name": "golang",
	}
	err = server.UpdateByIDTx(tx, tableName, upVals, 2)
	if err != nil {
		return
	}

	tx.Commit()
	record, err = server.SelectByID(tableName, 1)
	if err != nil {
		t.Fatal(err)
	}
	// 居然是pytho，都是5个字母，我怀疑是在更新出现了问题
	if len(record.Value) != 1 || record.Value[0] != "python" {
		t.Fatal("change data before commit expect python")
	}
	record, err = server.SelectByID(tableName, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 1 || record.Value[0] != "golang" {
		t.Fatal("change data before commit expect golang")
	}
}

func TestUpdateMulFieldInOneCommit(t *testing.T) {
	server := NewIDBServer()
	fms := []*FieldMeta{
		{
			name:         "name",
			isPrimaryKey: false,
			tp:           STRING,
		},
		{
			name:         "id",
			isPrimaryKey: false,
			tp:           STRING,
		},
	}

	tableName := "test"
	data := []interface{}{"hello", "1"}

	server.CreateTable(tableName, fms)
	err := server.Insert(tableName, data)
	if err != nil {
		t.Fatal(err)
	}

	tm := NewTxMgr(server, nil)
	tx := tm.StartTransaction()
	upVals := map[string]interface{}{
		"name": "world",
	}
	err = server.UpdateByIDTx(tx, tableName, upVals, 1)
	if err != nil {
		return
	}

	record, err := server.SelectByID(tableName, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 2 || record.Value[0] != "hello" || record.Value[1] != "1" {
		t.Fatal("change data before commit")
	}

	record, err = server.SelectByIDTx(tx, tableName, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 2 || record.Value[0] != "world" || record.Value[1] != "1" {
		t.Fatalf("unexpected %v", record)
	}

	tx.Commit()
	record, err = server.SelectByID(tableName, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 2 || record.Value[0] != "world" || record.Value[1] != "1" {
		t.Fatal("change data before commit")
	}
}

func TestSelectInTx(t *testing.T) {
	server := NewIDBServer()
	fms := []*FieldMeta{
		{
			name:         "name",
			isPrimaryKey: false,
			tp:           STRING,
		},
	}

	tableName := "test"

	server.CreateTable(tableName, fms)
	err := server.Insert(tableName, []interface{}{"hello"})
	if err != nil {
		t.Fatal(err)
	}
	err = server.Insert(tableName, []interface{}{"world"})
	if err != nil {
		t.Fatal(err)
	}
	err = server.Insert(tableName, []interface{}{"python"})
	if err != nil {
		t.Fatal(err)
	}

	record, err := server.SelectByID(tableName, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 1 || record.Value[0] != "hello" {
		t.Fatal("mismatch result")
	}

	tm := NewTxMgr(server, nil)
	tx := tm.StartTransaction()
	upVals := map[string]interface{}{
		"name": "world",
	}
	err = server.UpdateByIDTx(tx, tableName, upVals, 1)
	if err != nil {
		return
	}

	record, err = server.SelectByID(tableName, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 1 || record.Value[0] != "hello" {
		t.Fatal("change data before commit")
	}

	// select in tx
	record, err = server.SelectByIDTx(tx, tableName, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 1 || record.Value[0] != "world" {
		t.Fatal("change data before commit")
	}
	record, err = server.SelectByIDTx(tx, tableName, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 1 || record.Value[0] != "python" {
		t.Fatal("change data before commit")
	}

	tx.Rollback()
}

func TestTxBasicPath(t *testing.T) {
	server := NewIDBServer()
	fms := []*FieldMeta{
		{
			name:         "name",
			isPrimaryKey: false,
			tp:           STRING,
		},
	}

	tableName := "test"

	server.CreateTable(tableName, fms)

	// 插入元素1
	err := server.Insert(tableName, []interface{}{"hello"})
	if err != nil {
		t.Fatal(err)
	}

	record, err := server.SelectByID(tableName, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 1 || record.Value[0] != "hello" {
		t.Fatal("mismatch result")
	}

	// start tx1
	tm := NewTxMgr(server, nil)
	tx := tm.StartTransaction()
	var upVals map[string]interface{}

	// 插入元素2，能查到
	err = server.Insert(tableName, []interface{}{"world"})
	if err != nil {
		t.Fatal(err)
	}

	record, err = server.SelectByIDTx(tx, tableName, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 1 || record.Value[0] != "world" {
		t.Fatal("change data before commit")
	}

	// 更新元素1，能查到
	upVals = map[string]interface{}{
		"name": "xxxxx",
	}
	err = server.UpdateByIDTx(tx, tableName, upVals, 1)
	if err != nil {
		t.Fatal(err)
	}
	record, err = server.SelectByIDTx(tx, tableName, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 1 || record.Value[0] != "xxxxx" {
		t.Fatal("change data before commit expect xxxxx")
	}

	// 删除元素1，不能查到
	err = server.DeleteByIDTx(tx, tableName, 1)
	if err != nil {
		t.Fatal(err)
	}

	// TODO 先update，然后delete会导致根本没有这个cacheRecord，就只能去b+查
	record, err = server.SelectByIDTx(tx, tableName, 1)
	if err == nil || record != nil {
		t.Fatal("should not find")
	}

	// 删除元素2，不能查到2
	err = server.DeleteByIDTx(tx, tableName, 2)
	if err != nil {
		t.Fatal(err)
	}

	record, err = server.SelectByIDTx(tx, tableName, 2)
	if err == nil || record != nil {
		t.Fatal("should not find")
	}

	// 更新元素2，报错
	upVals = map[string]interface{}{
		"name": "yyyyy",
	}
	err = server.UpdateByIDTx(tx, tableName, upVals, 2)
	if err != ErrKeyNotFound {
		t.Fatal("更新一个不存在的record")
	}

	// 删除元素2，报错
	err = server.DeleteByIDTx(tx, tableName, 2)
	if err != ErrKeyNotFound {
		t.Fatal("更新一个不存在的record")
	}

	tx.Commit()

	// 确保不存在一个元素
	record, err = server.SelectByID(tableName, 1)
	if err != ErrKeyNotFound {
		t.Fatal("不应该找到1")
	}
	record, err = server.SelectByID(tableName, 2)
	if err != ErrKeyNotFound {
		t.Fatal("不应该找到2")
	}
}

// tx1 插入record2，tx1中查record2更改
// tx2 最小活跃id为invalid，查询record2，获取到最新record2
// tx3 最小活跃id为tx2，查询record2，获取到最新record2；更新record1
// tx4 最小活跃id为tx2，nextTxID为5，查询record2，获取到最新record2；查询record1，应该查到最新的；更新record2，删除record1;查询record3应该找不到
// tx5 活跃id为tx4。查询record2，此时应该查到tx4更新前的record2；查询record1应该查询到record1删除前的数据；
func TestIdbServer_SelectByIDTx(t *testing.T) {
	server := NewIDBServer()
	inspector := NewUndoInspector()
	server.WithOptions(func(option *ServerOptionConfig) {
		option.inspector = inspector
	})
	server.WithOptions()
	fms := []*FieldMeta{
		{
			name:         "name",
			isPrimaryKey: false,
			tp:           STRING,
		},
	}
	tableName := "test"
	server.CreateTable(tableName, fms)

	err := server.Insert(tableName, []interface{}{"hello"})
	if err != nil {
		t.Fatal(err)
	}

	// 找到的数据在缓存中
	tm := NewTxMgr(server, inspector)
	tx1 := tm.StartTransaction()

	err = server.InsertTx(tx1, tableName, []interface{}{"world"})
	if err != nil {
		t.Fatal(err)
	}
	record, err := server.SelectByIDTx(tx1, tableName, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 1 || record.Value[0] != "world" {
		t.Fatalf("unexpected %v", record.Value)
	}
	tx1.Commit()

	// 查询到记录小于最小活跃id
	tx2 := tm.StartTransaction()
	tx3 := tm.StartTransaction()
	record, err = server.SelectByIDTx(tx2, tableName, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 1 || record.Value[0] != "world" {
		t.Fatalf("unexpected %v", record.Value)
	}

	// 查询到记录不在活跃id，且小于下一个要分配的txID
	record, err = server.SelectByIDTx(tx3, tableName, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 1 || record.Value[0] != "world" {
		t.Fatalf("unexpected %v", record.Value)
	}

	tx3.Commit()

	// 根本就不存在该记录
	tx4 := tm.StartTransaction()
	tx2.Commit()
	tx5 := tm.StartTransaction()

	record, err = server.SelectByIDTx(tx4, tableName, 3)
	if err == nil {
		t.Fatal("根本不应该存在")
	}

	// 更新record数据txID不在活跃id中，且小于下一个txID
	record, err = server.SelectByIDTx(tx4, tableName, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 1 || record.Value[0] != "world" {
		t.Fatalf("unexpected %v", record.Value)
	}

	upVals := map[string]interface{}{
		"name": "yyyyy",
	}
	err = server.UpdateByIDTx(tx4, tableName, upVals, 2)
	if err != nil {
		t.Fatal(err)
	}

	err = server.DeleteByIDTx(tx4, tableName, 1)
	if err != nil {
		t.Fatal(err)
	}

	tx4.Commit()
	// 该记录查不到，但能从undoLog中找到。什么情况是bptree找不到，但undoLog能找到的呢？
	// 如果是按照当时查询的activeTx去找undoLogItem，这能保证两次查到的都是一致的嘛？并不能，若r1被删除，但未提交，第一次查到有，提交之后，第二次查到无
	// 其实还是能的。第一次查到之后会先缓存起来
	// 那么当初为什么用当前activeTx去SelectTx呢？用当前activeTx去SelectTx会导致永远会查到提交之后的数据
	record, err = server.SelectByIDTx(tx5, tableName, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 1 || record.Value[0] != "hello" {
		t.Fatalf("unexpected %v", record.Value)
	}

	// 该记录能查到，但查到的record被活跃tx修改过
	record, err = server.SelectByIDTx(tx5, tableName, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 1 || record.Value[0] != "world" {
		t.Fatalf("unexpected %v", record.Value)
	}
}

func TestEnsureTxIDNoRepeat(t *testing.T) {
	count := 1000
	server := NewIDBServer()
	inspector := NewUndoInspector()
	server.WithOptions(func(option *ServerOptionConfig) {
		option.inspector = inspector
	})
	server.WithOptions()
	fms := []*FieldMeta{
		{
			name:         "name",
			isPrimaryKey: false,
			tp:           STRING,
		},
	}
	tableName := "test"
	server.CreateTable(tableName, fms)

	// 先插入1000数据
	for i := 0; i < count; i++ {
		err := server.Insert(tableName, []interface{}{"hello"})
		if err != nil {
			t.Fatal(err)
		}
	}

	wg := &sync.WaitGroup{}
	wg.Add(count)
	tm := NewTxMgr(server, inspector)
	ch := make(chan int, count)
	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			tx := tm.StartTransaction()
			ch <- tx.id
		}()
	}
	wg.Wait()
	m := make(map[int]bool)
	close(ch)
	for id := range ch {
		if m[id] == true {
			t.Fatal("存在重复")
		}
		m[id] = true
	}
	t.Log("不存在重复")
}

func TestConcurrentCommit(t *testing.T) {
	count := 1000
	server := NewIDBServer()
	inspector := NewUndoInspector()
	server.WithOptions(func(option *ServerOptionConfig) {
		option.inspector = inspector
	})
	server.WithOptions()
	fms := []*FieldMeta{
		{
			name:         "name",
			isPrimaryKey: false,
			tp:           STRING,
		},
	}
	tableName := "test"
	server.CreateTable(tableName, fms)

	// 先插入1000数据
	for i := 0; i < count; i++ {
		err := server.Insert(tableName, []interface{}{"hello"})
		if err != nil {
			t.Fatal(err)
		}
	}

	wg := &sync.WaitGroup{}
	wg.Add(count * 2)
	tm := NewTxMgr(server, inspector)
	// 而后开启100goroutine去更新以及删除1000数据
	upVals := map[string]interface{}{
		"name": "world",
	}
	for i := 1; i <= count; i++ {
		i := i
		go func() {
			defer wg.Done()
			upTx := tm.StartTransaction()
			err := server.UpdateByIDTx(upTx, tableName, upVals, i)
			if err != nil {
				if err == ErrKeyNotFound {
					t.Logf("update key %d err key not found \n", i)
					upTx.Rollback()
					return
				}
				t.Error(err)
				return
			}
			err = upTx.Commit()
			if err != nil {
				t.Error(err)
				return
			}
		}()
		go func() {
			defer wg.Done()
			delTx := tm.StartTransaction()
			err := server.DeleteByIDTx(delTx, tableName, i)
			if err != nil {
				if err == ErrKeyNotFound {
					t.Logf("delete key %d err key not found \n", i)
					delTx.Rollback()
					return
				}
				t.Error(err)
				return
			}
			err = delTx.Commit()
			if err != nil {
				t.Error(err)
				return
			}
		}()
	}
	wg.Wait()
}
