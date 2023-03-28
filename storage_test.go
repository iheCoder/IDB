package IDB

import (
	"sync"
	"testing"
	"time"
)

func TestBasicInsertThenSelect(t *testing.T) {
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

	result, err := server.SelectByID(tableName, 1)
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Value) != 1 || result.Value[0] != "hello" {
		t.Fatal("mismatch result")
	}
}

func TestUpdateByID(t *testing.T) {
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

	result, err := server.SelectByID(tableName, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Value) != 1 || result.Value[0] != "hello" {
		t.Fatal("mismatch result")
	}

	upVals := map[string]interface{}{
		"name": "world",
	}
	err = server.UpdateByID(tableName, upVals, 1)
	if err != nil {
		t.Fatal(err)
	}

	result, err = server.SelectByID(tableName, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Value) != 1 || result.Value[0] != "world" {
		t.Fatal("mismatch result")
	}
}

func TestIdbServer_SelectByFields(t *testing.T) {
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
	err = server.Insert(tableName, []interface{}{"world1"})
	if err != nil {
		t.Fatal(err)
	}
	err = server.Insert(tableName, []interface{}{"world2"})
	if err != nil {
		t.Fatal(err)
	}
	err = server.Insert(tableName, []interface{}{"world3"})
	if err != nil {
		t.Fatal(err)
	}

	records, err := server.SelectByFields(tableName, map[string]interface{}{
		"name": "world1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].Value[0] != "world1" {
		t.Errorf("expected %v and got %v \n", "world1", records)
	}
}

func TestInsertALot(t *testing.T) {
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

	count := 1000
	wg := &sync.WaitGroup{}
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			err := server.Insert(tableName, data)
			if err != nil {
				t.Error(err)
				return
			}
		}()
	}
	wg.Wait()

	tt, ok := server.DB.tables[tableName]
	if !ok {
		t.Fatal("table not exist")
	}
	values, err := tt.data.FineByValue(func(record *Record) bool {
		return true
	})
	if err != nil {
		t.Fatal(err)
	}

	m := make(map[int]bool)
	for _, record := range values {
		if m[record.Key] {
			t.Fatal("repeat key")
		}
		m[record.Key] = true
	}
}

func TestUpdateALot(t *testing.T) {
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

	count := 100000
	wg := &sync.WaitGroup{}
	wg.Add(count)
	for i := 0; i < count; i++ {
		go func() {
			defer wg.Done()
			upVals := map[string]interface{}{
				"name": time.Now().String(),
			}
			err = server.UpdateByID(tableName, upVals, 1)
			if err != nil && err != ErrUpdateSame {
				t.Error(err)
				return
			}
		}()
	}
	wg.Wait()
}

func TestUpdateMulFields(t *testing.T) {
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

	upVals := map[string]interface{}{
		"name": "world",
	}
	err = server.UpdateByID(tableName, upVals, 1)
	if err != nil {
		t.Fatal(err)
	}
	record, err := server.SelectByID(tableName, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 2 || record.Value[0] != "world" || record.Value[1] != "1" {
		t.Fatalf("unexpected %v", record)
	}

	upVals = map[string]interface{}{
		"id": "2",
	}
	err = server.UpdateByID(tableName, upVals, 1)
	if err != nil {
		t.Fatal(err)
	}
	record, err = server.SelectByID(tableName, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Value) != 2 || record.Value[0] != "world" || record.Value[1] != "2" {
		t.Fatalf("unexpected %v", record)
	}
}

func TestConcurrentDeleteByID(t *testing.T) {
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

	count := 10000
	var err error
	for i := 1; i <= count; i++ {
		err = server.Insert(tableName, data)
		if err != nil {
			t.Fatal(err)
		}
	}

	wg := &sync.WaitGroup{}
	wg.Add(count)
	for i := 1; i <= count; i++ {
		i := i
		go func() {
			defer wg.Done()
			err = server.DeleteByID(tableName, i)
			if err != nil && err != ErrKeyNotFound {
				t.Error(err)
				return
			}
		}()
	}
	wg.Wait()
}
