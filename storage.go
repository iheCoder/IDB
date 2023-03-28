package IDB

import (
	"errors"
	"strconv"
	"sync/atomic"
)

type fieldType int

const (
	INT fieldType = iota
	STRING
)

const (
	InvalidLastTxID = 0
)

var (
	ErrTableNotExist        = errors.New("storage: table not exist")
	ErrRecordNotExist       = errors.New("storage: record not exist")
	ErrUnsupportedFieldType = errors.New("storage: unsupported filed type")
	ErrMismatchFieldType    = errors.New("storage: mismatched field type")
	ErrFieldNotExist        = errors.New("storage: field not exist")
	ErrFieldRequired        = errors.New("storage: field required")
	ErrInvalidOp            = errors.New("storage: invalid op")
)

type idbServer struct {
	DB     *db
	config *ServerConfig
}

type ServerConfig struct {
	options *ServerOptionConfig
}

type ServerOptionConfig struct {
	inspector Inspector
}

type db struct {
	name   string
	tables map[string]*table
}

type table struct {
	meta *tableMeta
	data *Tree
}

type tableMeta struct {
	idCount int64
	fields  []*FieldMeta
}

type FieldMeta struct {
	name         string
	isPrimaryKey bool
	tp           fieldType
	required     bool
}

func NewIDBServer() *idbServer {
	return &idbServer{
		DB: &db{
			name:   "main",
			tables: make(map[string]*table),
		},
		config: &ServerConfig{options: &ServerOptionConfig{}},
	}
}

type ServerOptionFunc func(option *ServerOptionConfig)

func (s *idbServer) WithOptions(opts ...ServerOptionFunc) {
	for _, optionFunc := range opts {
		optionFunc(s.config.options)
	}
}

func (s *idbServer) CreateTable(tableName string, fieldMetas []*FieldMeta) {
	t := &table{
		meta: &tableMeta{
			idCount: 0,
			fields:  fieldMetas,
		},
		data: s.createDataTree(),
	}
	s.DB.tables[tableName] = t
}

func (s *idbServer) createDataTree() *Tree {
	tree := NewTree()
	tree.WithInspector(s.config.options.inspector)
	return tree
}

func (s *idbServer) SelectByIDTx(tx *Tx, tableName string, id int) (*Record, error) {
	// 尝试从缓存中找到对应数据
	record, err := s.trySelectFromCache(tx, tableName, id)
	if err != nil {
		return nil, err
	}
	if record != nil {
		return record, nil
	}

	// 从b+树查询
	record, err = s.SelectByID(tableName, id)
	if err != nil && err != ErrKeyNotFound {
		return nil, err
	}

	// 若查询到的record小于最小活跃id或者（不在记录的活跃id中且小于下一个要分配的txID），直接返回查到的record
	if err == nil {
		lastTxID := record.Meta.LastTxID
		rv := tx.rv
		if rv.minActiveID == InvalidLastTxID || lastTxID < rv.minActiveID || (!rv.activeTxIDs[lastTxID] && lastTxID < rv.nextTxID) {
			return record, nil
		}
	}

	// 到undoLog中查询。若找不到更改，就直接返回查到的最新record
	ur, ferr := tx.mgr.FindRecordInUndoLog(tableName, id, tx.rv.activeTxIDs)
	if ferr == ErrRecordNotCommit {
		if err == ErrKeyNotFound {
			return nil, ErrKeyNotFound
		}
		return record, nil
	}
	if ferr != nil {
		return nil, ferr
	}
	if ur == nil {
		return nil, ErrKeyNotFound
	}
	return ur, nil
}

// trySelectFromCache 尝试从缓存中找到对应数据
func (s *idbServer) trySelectFromCache(tx *Tx, tableName string, id int) (*Record, error) {
	c, ok := tx.cache[tableName]
	if ok {
		opRecord := c.cache[id]
		if opRecord != nil {
			switch opRecord.op {
			// 若被删除了就返回nil
			case DELETE:
				return nil, ErrKeyNotFound

			case INSERT:
				return opRecord.opChange.(*InsertOpChange).record, nil

			case UPDATE:
				opChange := opRecord.opChange.(*UpdateOpChange)
				if opChange.record != nil {
					return opChange.record, nil
				}
				record, err := s.SelectByID(tableName, id)
				if err != nil {
					return nil, err
				}
				values := make([]string, len(record.Value))
				copy(values, record.Value)
				for i, v := range opChange.change {
					values[i] = v
				}
				r := &Record{
					Key:   id,
					Value: values,
				}
				opChange.record = r
				return r, nil

			}
		}
	}

	return nil, nil
}

// SelectByID 根据id以及表名查询数据
func (s *idbServer) SelectByID(tableName string, id int) (*Record, error) {
	// 找到对应表
	t, ok := s.DB.tables[tableName]
	if !ok {
		return nil, ErrTableNotExist
	}

	// 从B+树中找到对应id数据
	record, err := t.data.Find(id)
	if err != nil {
		return nil, err
	}

	return record, nil
}

func (s *idbServer) SelectByFields(tableName string, conds map[string]interface{}) ([]*Record, error) {
	// 找到对应表
	t, ok := s.DB.tables[tableName]
	if !ok {
		return nil, ErrTableNotExist
	}

	// 构造isTarget方法
	fields := t.meta.fields
	fieldNameIndexCache := make(map[string]int)
	isTargets := make([]IsTarget, 0)
	var i, startIndex int
	for key, cond := range conds {
		i, ok = fieldNameIndexCache[key]
		if !ok {
			for ; startIndex < len(fields); startIndex++ {
				fieldNameIndexCache[fields[startIndex].name] = startIndex
				if fields[startIndex].name == key {
					i = startIndex
					break
				}
			}
			if startIndex >= len(fields) {
				return nil, ErrFieldNotExist
			}
		}

		isTargets = append(isTargets, func(record *Record) bool {
			if record.Value[i] == cond {
				return true
			}
			return false
		})
	}

	isTarget := func(record *Record) bool {
		for _, it := range isTargets {
			if !it(record) {
				return false
			}
		}
		return true
	}

	// 调用findByValue
	return t.data.FineByValue(isTarget)
}

func (s *idbServer) UpdateByIDTx(tx *Tx, tableName string, values map[string]interface{}, id int) error {
	var opRecord *OpRecord
	var t *table
	var err error
	// 找到表tx缓存
	c, err := s.findTableTxCache(tx, tableName)
	if err != nil {
		return err
	}
	t = c.t

	// 尝试找到cache opRecord，并更新op
	opRecord = c.cache[id]
	if opRecord != nil {
		// 若记录操作为删除，则报错
		if opRecord.op == DELETE {
			return ErrKeyNotFound
		}
	} else {
		// 若缓存中无法找到该记录且在bptree不存在，则报错
		_, err = t.data.Find(id)
		if err != nil {
			return err
		}

		// 若在bptree存在，则缓存新添加更新opRecord
		opRecord = &OpRecord{
			opChange: &UpdateOpChange{
				change: make(map[int]string),
			},
			op: UPDATE,
		}
		c.cache[id] = opRecord
	}

	// 若前操作为insert，那么操作仍然为insert。只是里面的record进行更新
	// 若前操作无或者为update，那么操作为update。record若存在则更新record，否则添加更新
	// TODO 原来的record还有记录txID之用. 若UpdateOpChange已经有record呢
	err = wrapOpRecordWhenUpdate(t, values, opRecord)
	if err != nil {
		return err
	}
	opRecord.LastTxID = tx.id

	// 记录更新后的record
	c.cache[id] = opRecord

	return nil
}

// wrapOpRecordWhenUpdate 获取更新后的record
func wrapOpRecordWhenUpdate(t *table, values map[string]interface{}, opRecord *OpRecord) error {
	data, err := convValuesToBPlusData(t, values)
	if err != nil {
		return err
	}

	switch opRecord.op {
	case INSERT:
		opChange := opRecord.opChange.(*InsertOpChange)
		for i, v := range data {
			opChange.record.Value[i] = v
		}

	case UPDATE:
		opChange := opRecord.opChange.(*UpdateOpChange)
		for i, v := range data {
			opChange.change[i] = v
		}
		if opChange.record == nil {
			break
		}
		for i, v := range data {
			opChange.record.Value[i] = v
		}

	default:
		return ErrInvalidOp

	}

	return nil
}

// TODO commit 不是原子的，若tx2更新前已经被tx1删除，那么tx1之前的操作不会回滚，后面的操作也不会提交
func (s *idbServer) commit(cache map[string]*txCache) error {
	var err error
	for _, c := range cache {
		for key, rc := range c.cache {
			switch rc.op {
			case UPDATE:
				// TODO 怎么更新record的lastTxID呢
				opChange := rc.opChange.(*UpdateOpChange)
				err = c.t.data.UpdateRecord(opChange.change, key, func(meta *RecordMeta) *RecordMeta {
					meta.LastTxID = rc.LastTxID
					return meta
				})
				// 可能出现无法原子commit的关键在于更新的记录可能被删除，导致了后面操作无法commit，那就忽略这个错误就不会出现原子问题
				// commit更新record，出现错误，就没有HandleDataBeforeUpdate，可是afterCommit却并不知道这个记录没有还是去找了，导致了nil panic
				// (1 让tx传进来然后删除对应txCache，那么afterCommit就当作这个提交不存在 (2 忽略不存在的txData
				if err != nil && err != ErrKeyNotFound && err != ErrUpdateSame {
					return err
				}

			case INSERT:
				err = c.t.data.Insert(rc.opChange.(*InsertOpChange).record)
				if err != nil {
					return err
				}

			case DELETE:
				err = c.t.data.Delete(key)
				// 还要忽略删除时的ErrKeyNotFound
				if err != nil && err != ErrKeyNotFound {
					return err
				}

			}
		}
	}

	return nil
}

func (s *idbServer) UpdateByID(tableName string, values map[string]interface{}, id int) error {
	// 找到对应表
	t, ok := s.DB.tables[tableName]
	if !ok {
		return ErrTableNotExist
	}

	// 将更新数据转化为string类型
	data, err := convValuesToBPlusData(t, values)
	if err != nil {
		return err
	}

	// 更新数据
	return t.data.UpdateRecord(data, id, nil)
}

func convValuesToBPlusData(t *table, values map[string]interface{}) (map[int]string, error) {
	fields := t.meta.fields
	data := make(map[int]string)
	fieldNameIndexCache := make(map[string]int)
	var v string
	var err error
	var i, startIndex int
	var ok bool
	for key, value := range values {
		i, ok = fieldNameIndexCache[key]
		if !ok {
			for ; startIndex < len(fields); startIndex++ {
				fieldNameIndexCache[fields[startIndex].name] = startIndex
				if fields[startIndex].name == key {
					i = startIndex
					break
				}
			}
			if startIndex >= len(fields) {
				return nil, ErrFieldNotExist
			}
		}

		v, err = convertValueToString(fields[i].tp, value)
		if err != nil {
			return nil, err
		}
		data[i] = v
	}

	return data, nil
}

func convertValueToString(ft fieldType, v interface{}) (string, error) {
	if v == nil {
		return "", nil
	}
	switch ft {
	case STRING:
		r, ok := v.(string)
		if !ok {
			return "", ErrMismatchFieldType
		}
		return r, nil
	case INT:
		r, ok := v.(int)
		if !ok {
			return "", ErrMismatchFieldType
		}
		return strconv.Itoa(r), nil
	default:
		return "", ErrUnsupportedFieldType
	}
}

// Insert 插入数据。自动递增主键
func (s *idbServer) Insert(tableName string, data []interface{}) error {
	// 找到对应表
	t, ok := s.DB.tables[tableName]
	if !ok {
		return ErrTableNotExist
	}

	// 检查插入数据类型一致
	innerData, err := convDataToStorageData(t.meta.fields, data)
	if err != nil {
		return err
	}

	// 递增表主键
	r := &Record{
		Key:   int(atomic.AddInt64(&(t.meta.idCount), 1)),
		Value: innerData,
		Meta:  &RecordMeta{},
	}
	return t.data.Insert(r)
}

func convDataToStorageData(fields []*FieldMeta, data []interface{}) ([]string, error) {
	if len(fields) != len(data) {
		return nil, ErrFieldRequired
	}
	var d interface{}
	var err error
	innerData := make([]string, len(fields))
	for i := 0; i < len(fields); i++ {
		d = data[i]
		if d == nil && fields[i].required {
			return nil, ErrFieldRequired
		}
		innerData[i], err = convertValueToString(fields[i].tp, d)
		if err != nil {
			return nil, err
		}
	}

	return innerData, nil
}

func (s *idbServer) InsertTx(tx *Tx, tableName string, data []interface{}) error {
	// 找到表
	c, err := s.findTableTxCache(tx, tableName)
	if err != nil {
		return err
	}
	t := c.t

	// 获取record id
	id := int(atomic.AddInt64(&(t.meta.idCount), 1))

	// 构造record
	innerData, err := convDataToStorageData(t.meta.fields, data)
	if err != nil {
		return err
	}
	record := &Record{
		Key:   id,
		Value: innerData,
		Meta:  &RecordMeta{LastTxID: tx.id},
	}
	tx.cache[tableName].cache[id] = &OpRecord{
		opChange: &InsertOpChange{record: record},
		op:       INSERT,
		LastTxID: tx.id,
	}

	return nil
}

func (s *idbServer) DeleteByIDTx(tx *Tx, tableName string, id int) error {
	// 找到表
	c, err := s.findTableTxCache(tx, tableName)
	if err != nil {
		return err
	}
	t := c.t

	// 尝试找到recordCache
	var record *Record
	cr := c.cache[id]
	// 若recordCache存在，则记录操作为删除就报错，为其他就直接删除该recordCache
	if cr != nil {
		// 若找到的cache record存在，则判断是否也是删除操作
		if cr.op == DELETE {
			return ErrKeyNotFound
		}

		// 若记录操作为insert，则删除该record
		if cr.op == INSERT {
			delete(c.cache, id)
			return nil
		}
	}

	// 添加删除recordCache
	record, err = t.data.Find(id)
	if err != nil {
		return err
	}

	// 添加删除cache
	c.cache[id] = &OpRecord{
		opChange: &DeleteOpChange{id: record.Key},
		op:       DELETE,
	}

	return nil
}

func (s *idbServer) findTableTxCache(tx *Tx, tableName string) (*txCache, error) {
	var t *table
	// 尝试在缓存中找到表
	c, ok := tx.cache[tableName]
	if ok {
		t = c.t
	} else {
		// 找到对应表
		t, ok = s.DB.tables[tableName]
		if !ok {
			return nil, ErrTableNotExist
		}

		// 增加该表对应的txCache
		c = &txCache{
			t:     t,
			cache: make(map[int]*OpRecord),
		}
		tx.cache[tableName] = c
	}
	return c, nil
}

func (s *idbServer) DeleteByID(tableName string, id int) error {
	// 找到对应表
	t, ok := s.DB.tables[tableName]
	if !ok {
		return ErrTableNotExist
	}

	// 删除b+树中数据
	return t.data.Delete(id)
}
