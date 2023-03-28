package IDB

import "testing"

//func TestAppend(t *testing.T) {
//	l := NewUndoLog()
//	maIDs := map[int]bool{
//		1: true,
//	}
//	records := []*UndoRecord{
//		{
//			record: &Record{Key: 1},
//			op:     InvalidTxID,
//		},
//	}
//
//	txIds := []int{6, 5, 2, 1}
//
//	l.Append(5, maIDs, records)
//	l.Append(1, maIDs, records)
//	l.Append(6, maIDs, records)
//	l.Append(2, maIDs, records)
//
//	p := l.start
//	var i int
//	for p != nil {
//		if p.commitTxID != txIds[i] {
//			t.Fatalf("expected %d got %d", txIds[i], p.commitTxID)
//		}
//		i++
//		p = p.next
//	}
//}

// 假设tx1、2、3按照顺序开始事务，在tx3开启之后tx1、tx2、tx3依次commit。tx4在tx1提交之后开始事务；
func TestUndoLogBasicPath(t *testing.T) {
	// 添加。 tx3, maIDs:1,2 records:ins r1
	ul := NewUndoLog()

	// 根本不存在该记录
	_, err := ul.Find(make(map[int]bool), 1)
	if err != ErrRecordNotCommit {
		t.Fatal("根本不应该找到")
	}

	rs1 := []*UndoRecord{
		{
			record: &Record{
				Key:   1,
				Value: []string{"hello"},
			},
		},
	}
	rs2 := []*UndoRecord{
		{
			record: &Record{
				Key:   2,
				Value: []string{"world"},
			},
		},
	}
	ul.Append(1, map[int]bool{2: true, 3: true}, rs1)

	// 假设存在一个tx4，在tx1提交之后才开始，那么应该要查到最新记录。也就是Find应该是ErrRecordNotCommit
	_, err = ul.Find(map[int]bool{2: true, 3: true}, 1)
	if err != ErrRecordNotCommit {
		t.Fatal("tx4 根本不应该找到")
	}

	// tx2应该找的到record 1
	// TODO 似乎别人影响他和他影响别人的，都是同一个别人啊
	record, err := ul.Find(map[int]bool{1: true, 3: true}, 1)
	if err != nil {
		t.Fatalf("应该找到 but %s", err)
	}
	if len(record.Value) != 1 || record.Value[0] != "hello" {
		t.Fatalf("got %v", record)
	}

	ul.Append(2, map[int]bool{3: true}, rs2)

	aids3 := map[int]bool{
		1: true,
		2: true,
	}
	record, err = ul.Find(aids3, 2)
	if err != nil {
		t.Fatalf("应该找到 but %s", err)
	}
	if len(record.Value) != 1 || record.Value[0] != "world" {
		t.Fatalf("got %v", record)
	}

	// 活跃tx改动过
	// 有些怪异。如果tx1记录了活跃tx2，nextTxID为3。那么当tx2提交之后是不应该看到tx2更改的记录，更不应该看到tx3以及之后的提交记录
	// 所以找到的记录应该是最早更新过该record的活跃tx undo record
	// tx3应该找不到record1
	record, err = ul.Find(aids3, 1)
	if err != nil {
		t.Fatalf("应该找到 but %s", err)
	}
	if len(record.Value) != 1 || record.Value[0] != "hello" {
		t.Fatalf("got %v", record)
	}

	// tx4 应该找到record 2
	record, err = ul.Find(map[int]bool{2: true, 3: true}, 2)
	if err != nil {
		t.Fatalf("tx4应该找到2 but %s", err)
	}
	if len(record.Value) != 1 || record.Value[0] != "world" {
		t.Fatalf("got %v", record)
	}
}

func TestReclaim(t *testing.T) {
	ul := NewUndoLog()
	rs1 := []*UndoRecord{
		{
			record: &Record{
				Key:   1,
				Value: []string{"hello"},
			},
		},
	}

	// tx1添加可能被影响事务2、3。此时1的item、对应record的recordCache、affectTxItems应该都在
	aids1 := map[int]bool{
		2: true,
		3: true,
	}
	ul.Append(1, aids1, rs1)
	if len(ul.recordsCache) != 1 || len(ul.affectTxItems) != 2 || len(ul.items) != 1 {
		t.Fatal("tx1 缓存items数量不对")
	}

	// tx2添加可能影响事务3。此时1、2的item、对应record的recordCache、affectTxItems应该都在
	aids2 := map[int]bool{
		3: true,
	}
	ul.Append(2, aids2, rs1)
	if len(ul.recordsCache) != 1 || len(ul.affectTxItems) != 1 || len(ul.items) != 2 {
		t.Fatal("tx2 缓存items数量不对")
	}

	// tx3添加可能影响事务无。此时item、对应record的recordCache、affectTxItems应该都不在
	aids3 := map[int]bool{}
	ul.Append(3, aids3, rs1)
	if len(ul.recordsCache) != 0 || len(ul.affectTxItems) != 0 || len(ul.items) != 0 {
		t.Fatal("tx3 缓存items数量不对")
	}
}
