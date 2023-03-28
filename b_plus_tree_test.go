package IDB

import (
	"fmt"
	"gotest.tools/v3/assert"
	"reflect"
	"testing"
)

func TestBPlusBasicPath(t *testing.T) {
	bpt := NewBPTreeDep(3)
	bpt.Set(1, "hello")
	assert.Equal(t, bpt.Get(1), "hello")

	bpt.Set(1, "world")
	assert.Equal(t, bpt.Get(1), "world")

	bpt.Remove(1)
	assert.Equal(t, bpt.Get(1), nil)
}

func TestDetailNewLeafNode(t *testing.T) {
	items := make([]int, 3)
	fmt.Println(items)
	items = items[0:0]
	fmt.Println(items)
}

func hello() {
	fmt.Println("bptree says 'hello friend'")
}

func TestInsertNilRoot(t *testing.T) {
	tree := NewTree()
	hello()

	key := 1
	value := []string{"test"}

	ir := &Record{
		Key:   key,
		Value: value,
	}

	err := tree.Insert(ir)

	if err != nil {
		t.Errorf("%s", err)
	}

	r, err := tree.Find(key)
	if err != nil {
		t.Errorf("%s\n", err)
	}

	if r == nil {
		t.Errorf("returned nil \n")
	}

	if !reflect.DeepEqual(r.Value, value) {
		t.Errorf("expected %v and got %v \n", value, r.Value)
	}
}

func TestInsert(t *testing.T) {
	tree := NewTree()

	key := 1
	value := []string{"test"}

	ir := &Record{
		Key:   key,
		Value: value,
	}
	err := tree.Insert(ir)
	if err != nil {
		t.Errorf("%s", err)
	}

	r, err := tree.Find(key)
	if err != nil {
		t.Errorf("%s\n", err)
	}

	if r == nil {
		t.Errorf("returned nil \n")
	}

	if !reflect.DeepEqual(r.Value, value) {
		t.Errorf("expected %v and got %v \n", value, r.Value)
	}
}

func TestInsertSameKeyTwice(t *testing.T) {
	tree := NewTree()

	key := 1
	value := []string{"test"}
	ir := &Record{
		Key:   key,
		Value: value,
	}

	err := tree.Insert(ir)
	if err != nil {
		t.Errorf("%s", err)
	}

	err = tree.Insert(ir)
	if err == nil {
		t.Errorf("expected error but got nil")
	}

	r, err := tree.Find(key)
	if err != nil {
		t.Errorf("%s\n", err)
	}

	if r == nil {
		t.Errorf("returned nil \n")
	}

	if !reflect.DeepEqual(r.Value, value) {
		t.Errorf("expected %v and got %v \n", value, r.Value)
	}

	if tree.Root.NumKeys > 1 {
		t.Errorf("expected 1 key and got %d", tree.Root.NumKeys)
	}
}

func TestFindNilRoot(t *testing.T) {
	tree := NewTree()

	r, err := tree.Find(1)
	if err == nil {
		t.Errorf("expected error and got nil")
	}

	if r != nil {
		t.Errorf("expected nil got %v \n", r)
	}
}

//
//func TestDeleteNilTree(t *testing.T) {
//	tree := NewTree()
//
//	key := 1
//
//	err := tree.Delete(key)
//	if err == nil {
//		t.Errorf("expected error and got nil")
//	}
//
//	r, err := tree.Find(key, false)
//	if err == nil {
//		t.Errorf("expected error and got nil")
//	}
//
//	if r != nil {
//		t.Errorf("returned struct after delete \n")
//	}
//}
//
//func TestDelete(t *testing.T) {
//	tree := NewTree()
//
//	key := 1
//	value := []byte("test")
//
//	err := tree.Insert(key, value)
//	if err != nil {
//		t.Errorf("%s", err)
//	}
//
//	r, err := tree.Find(key, false)
//	if err != nil {
//		t.Errorf("%s\n", err)
//	}
//
//	if r == nil {
//		t.Errorf("returned nil \n")
//	}
//
//	if !reflect.DeepEqual(r.Value, value) {
//		t.Errorf("expected %v and got %v \n", value, r.Value)
//	}
//
//	err = tree.Delete(key)
//	if err != nil {
//		t.Errorf("%s\n", err)
//	}
//
//	r, err = tree.Find(key, false)
//	if err == nil {
//		t.Errorf("expected error and got nil")
//	}
//
//	if r != nil {
//		t.Errorf("returned struct after delete \n")
//	}
//}
//
//func TestDeleteNotFound(t *testing.T) {
//	tree := NewTree()
//
//	key := 1
//	value := []byte("test")
//
//	err := tree.Insert(key, value)
//	if err != nil {
//		t.Errorf("%s", err)
//	}
//
//	r, err := tree.Find(key, false)
//	if err != nil {
//		t.Errorf("%s\n", err)
//	}
//
//	if r == nil {
//		t.Errorf("returned nil \n")
//	}
//
//	if !reflect.DeepEqual(r.Value, value) {
//		t.Errorf("expected %v and got %v \n", value, r.Value)
//	}
//
//	err = tree.Delete(key + 1)
//	if err == nil {
//		t.Errorf("expected error and got nil")
//	}
//
//	r, err = tree.Find(key+1, false)
//	if err == nil {
//		t.Errorf("expected error and got nil")
//	}
//}
//
func TestMultiInsertSingleDelete(t *testing.T) {
	tree := NewTree()

	key := 1
	value := []string{"test"}

	r1 := &Record{
		Key:   key,
		Value: value,
	}
	err := tree.Insert(r1)
	if err != nil {
		t.Errorf("%s", err)
	}
	r2 := &Record{
		Key:   key + 1,
		Value: []string{"world1"},
	}
	err = tree.Insert(r2)
	if err != nil {
		t.Errorf("%s", err)
	}
	r3 := &Record{
		Key:   key + 2,
		Value: []string{"world2"},
	}
	err = tree.Insert(r3)
	if err != nil {
		t.Errorf("%s", err)
	}
	r4 := &Record{
		Key:   key + 3,
		Value: []string{"world3"},
	}
	err = tree.Insert(r4)
	if err != nil {
		t.Errorf("%s", err)
	}
	r5 := &Record{
		Key:   key + 4,
		Value: append(value, "world3"),
	}
	err = tree.Insert(r5)
	if err != nil {
		t.Errorf("%s", err)
	}

	r, err := tree.Find(key)
	if err != nil {
		t.Errorf("%s\n", err)
	}

	if r == nil {
		t.Errorf("returned nil \n")
	}

	if !reflect.DeepEqual(r.Value, value) {
		t.Errorf("expected %v and got %v \n", value, r.Value)
	}

	//err = tree.Delete(key)
	//if err != nil {
	//	t.Errorf("%s\n", err)
	//}
	//
	//r, err = tree.Find(key, false)
	//if err == nil {
	//	t.Errorf("expected error and got nil")
	//}
	//
	//if r != nil {
	//	t.Errorf("returned struct after delete - %v \n", r)
	//}
}

func TestFindByValue(t *testing.T) {
	tree := NewTree()

	key := 1
	value := []string{"test"}

	r1 := &Record{
		Key:   key,
		Value: value,
	}
	err := tree.Insert(r1)
	if err != nil {
		t.Errorf("%s", err)
	}
	r2 := &Record{
		Key:   key + 1,
		Value: []string{"world1"},
	}
	err = tree.Insert(r2)
	if err != nil {
		t.Errorf("%s", err)
	}
	r3 := &Record{
		Key:   key + 2,
		Value: []string{"world2"},
	}
	err = tree.Insert(r3)
	if err != nil {
		t.Errorf("%s", err)
	}
	r4 := &Record{
		Key:   key + 3,
		Value: []string{"world3"},
	}
	err = tree.Insert(r4)
	if err != nil {
		t.Errorf("%s", err)
	}
	r5 := &Record{
		Key:   key + 4,
		Value: append(value, "world3"),
	}
	err = tree.Insert(r5)
	if err != nil {
		t.Errorf("%s", err)
	}

	isTarget := func(record *Record) bool {
		if record.Value[0] == "world3" {
			return true
		}
		return false
	}
	result, err := tree.FineByValue(isTarget)
	if err != nil {
		t.Fatal(err)
	}

	if len(result) != 1 || result[0].Value[0] != "world3" {
		t.Errorf("expected %v and got %v \n", "world3", result)
	}
}

func TestMultiInsertMultiDelete(t *testing.T) {
	tree := NewTree()

	key := 1
	value := []string{"test"}

	r1 := &Record{
		Key:   key,
		Value: value,
	}
	err := tree.Insert(r1)
	if err != nil {
		t.Errorf("%s", err)
	}
	r2 := &Record{
		Key:   key + 1,
		Value: []string{"world1"},
	}
	err = tree.Insert(r2)
	if err != nil {
		t.Errorf("%s", err)
	}
	r3 := &Record{
		Key:   key + 2,
		Value: []string{"world2"},
	}
	err = tree.Insert(r3)
	if err != nil {
		t.Errorf("%s", err)
	}
	r4 := &Record{
		Key:   key + 3,
		Value: []string{"world3"},
	}
	err = tree.Insert(r4)
	if err != nil {
		t.Errorf("%s", err)
	}
	r5 := &Record{
		Key:   key + 4,
		Value: append(value, "world3"),
	}
	err = tree.Insert(r5)
	if err != nil {
		t.Errorf("%s", err)
	}

	r, err := tree.Find(key)
	if err != nil {
		t.Errorf("%s\n", err)
	}

	if r == nil {
		t.Errorf("returned nil \n")
	}

	if !reflect.DeepEqual(r.Value, value) {
		t.Errorf("expected %v and got %v \n", value, r.Value)
	}

	err = tree.Delete(key)
	if err != nil {
		t.Errorf("%s\n", err)
	}

	r, err = tree.Find(key)
	if err == nil {
		t.Errorf("expected error and got nil")
	}

	if r != nil {
		t.Errorf("returned struct after delete - %v \n", r)
	}

	r, err = tree.Find(key + 3)
	if err != nil {
		t.Errorf("%s\n", err)
	}

	if r == nil {
		t.Errorf("returned nil \n")
	}

	if !reflect.DeepEqual(r.Value, append(value, "world3")) {
		t.Errorf("expected %v and got %v \n", value, r.Value)
	}

	err = tree.Delete(key + 3)
	if err != nil {
		t.Errorf("%s\n", err)
	}

	r, err = tree.Find(key + 3)
	if err == nil {
		t.Errorf("expected error and got nil")
	}

	if r != nil {
		t.Errorf("returned struct after delete - %v \n", r)
	}
}

func TestALotInsertThenFind(t *testing.T) {
	tree := NewTree()

	key := 1
	value := []string{"test"}
	r := &Record{
		Value: value,
		lock:  nil,
	}

	count := 100
	for i := 0; i < count; i++ {
		err := tree.Insert(r)
		if err != nil {
			t.Errorf("%s", err)
			return
		}
		key++
	}

	for i := 0; i < count; i++ {
		r, err := tree.Find(i + 1)
		if err != nil {
			t.Errorf("key %d expected error %v and got nil", i+1, r)
		}
		if r == nil {
			t.Errorf("returned struct after insert - %v \n", r)
		}
	}
}

func TestALotInsertALotDel(t *testing.T) {
	tree := NewTree()

	key := 1
	value := []string{"test"}
	r := &Record{
		Value: value,
		lock:  nil,
	}

	count := 100
	for i := 0; i < count; i++ {
		err := tree.Insert(r)
		if err != nil {
			t.Errorf("%s", err)
			return
		}
		key++
	}

	for i := 0; i < count; i++ {
		err := tree.Delete(i + 1)
		if err != nil {
			t.Errorf("%s\n", err)
			return
		}
		r, err := tree.Find(i + 1)
		if err == nil {
			t.Errorf("expected error and got nil")
			return
		}
		if r != nil {
			t.Errorf("returned struct after delete - %v \n", r)
			return
		}
	}
}

func TestTryUpdateValue(t *testing.T) {
	//r := &Record{
	//	Key:   0,
	//	Value: []string{"hello"},
	//}
	//tryUpdateValue(r, 0, "world")
	//fmt.Println(r.Value)
	//tryUpdateValue(r, 0, "world1")
	//fmt.Println(r.Value)
}
