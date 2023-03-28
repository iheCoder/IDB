package IDB

import (
	"errors"
	"reflect"
	"sync"
)

var (
	ErrKeyExists     = errors.New("key exists")
	ErrKeyNotFound   = errors.New("key not found")
	ErrValueNotFound = errors.New("value not found")
	ErrUpdateSame    = errors.New("update same")
	ErrNoSuchChild   = errors.New("no such child")
)

var (
	order = 3
)

type Inspector interface {
	// HandleDataBeforeUpdate 在更新前处理原来和更新后的数据
	HandleDataBeforeUpdate(oldRecord *Record, newRecordMeta *RecordMeta)
	// HandleDataBeforeDelete 在删除前处理原来的数据
	HandleDataBeforeDelete(record *Record)
}

type IsTarget func(record *Record) bool

// is Tree balance? not yet
type Tree struct {
	Root       *Node
	mu         *sync.RWMutex
	recordLock *sync.RWMutex
	inspector  Inspector
}

// pointer 0, 1, 2 ... last point to sliding(count n+1)
// keys    0, 1, 2 ... (count n)
type Node struct {
	// 子节点。最后一位指向右兄弟节点
	Pointers []interface{}
	// 非叶节点。key为对应pointer的key
	Keys    []int
	Parent  *Node
	IsLeaf  bool
	NumKeys int
	Next    *Node
}

type Record struct {
	Key     int
	Value   []string
	lock    *sync.Mutex
	Meta    *RecordMeta
	deleted bool
}

type RecordMeta struct {
	// 记录上次更新该record的txID。
	LastTxID int
}

func NewTree() *Tree {
	return &Tree{
		mu:         &sync.RWMutex{},
		recordLock: &sync.RWMutex{},
	}
}

func (t *Tree) WithInspector(inspector Inspector) {
	t.inspector = inspector
}

func (t *Tree) Insert(record *Record) error {
	key := record.Key
	// 尝试找到key，若能找到则返回已经存在的错误
	if _, err := t.Find(key); err == nil {
		return ErrKeyExists
	}

	// 若根节点为空，则创建新树
	// 锁。若thread1创建好了，可是thread2看到的却还是nil，这就会出问题
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.Root == nil {
		t.createNewTree(key, record)
		return nil
	}

	// 找到key对应的叶节点
	leaf := t.findLeaf(key)

	// 若找到叶节点数量小于order直接插入
	if leaf.NumKeys < order-1 {
		insertIntoLeaf(leaf, key, record)
		return nil
	}

	t.insertIntoLeafAfterSplitting(leaf, key, record)
	return nil
}

// insertIntoLeaf 插入叶节点
func insertIntoLeaf(leaf *Node, key int, r *Record) {
	// 在叶节点找到第一个大于等于key的元素为插入点
	var insertPoint int
	for insertPoint < leaf.NumKeys && leaf.Keys[insertPoint] < key {
		insertPoint++
	}

	// 将插入点后面元素全部往后移动一位
	for i := leaf.NumKeys; i > insertPoint; i-- {
		leaf.Keys[i] = leaf.Keys[i-1]
		leaf.Pointers[i] = leaf.Pointers[i-1]
	}

	// 插入元素
	leaf.Keys[insertPoint] = key
	leaf.Pointers[insertPoint] = r
	leaf.NumKeys++
}

// insertIntoLeafAfterSplitting 在叶节点超出限制之后，迁移右边一半到新节点
func (t *Tree) insertIntoLeafAfterSplitting(leaf *Node, key int, r *Record) {
	nl := makeLeaf()
	keys := make([]int, order+1)
	vals := make([]interface{}, order+1)

	// 在叶节点找到第一个大于等于key的元素或者最后一个为插入点
	var insertPoint int
	for insertPoint < order-1 && leaf.Keys[insertPoint] < key {
		insertPoint++
	}

	// 将叶节点以及插入元素迁移到临时keys、vals
	var i, j int
	for i = 0; i < leaf.NumKeys; i++ {
		if j == insertPoint {
			j++
		}
		keys[j] = leaf.Keys[i]
		vals[j] = leaf.Pointers[i]
		j++
	}
	keys[insertPoint] = key
	vals[insertPoint] = r

	leaf.NumKeys = 0
	// 从临时keys、vals插入前一半到叶节点
	split := cut(order - 1)
	for i = 0; i < split; i++ {
		leaf.Pointers[i] = vals[i]
		leaf.Keys[i] = keys[i]
		leaf.NumKeys++
	}

	// 从临时keys、vals插入后一半到新叶节点
	j = 0
	for k := split; k < order; k++ {
		nl.Pointers[j] = vals[k]
		nl.Keys[j] = keys[k]
		nl.NumKeys++
		j++
	}

	// 使新叶节点成为原节点的右节点
	nl.Pointers[order] = leaf.Pointers[order]
	leaf.Pointers[order] = nl

	// 让原叶节点、新叶节点后半部分全为nil
	for i = leaf.NumKeys; i < order; i++ {
		leaf.Pointers[i] = nil
	}
	for i = nl.NumKeys; i < order; i++ {
		nl.Pointers[i] = nil
	}

	// 使新节点parent为原节点parent
	nl.Parent = leaf.Parent

	// 将新叶节点插入父节点
	t.insertIntoParent(leaf, nl.Keys[nl.NumKeys-1], nl)
}

// insertIntoParent 插入右节点到parent
func (t *Tree) insertIntoParent(left *Node, key int, right *Node) {
	// 若parent为nil，则新建parent插入
	parent := left.Parent
	if parent == nil {
		t.insertIntoNewRoot(left, right)
		return
	}

	// 更新left key
	// TODO 困扰我这么久的为什么找到key居然是被删除了的
	leftIndex := getNodeIndex(parent, left)
	parent.Keys[leftIndex] = left.Keys[left.NumKeys-1]

	// 若父节点子节点未满，直接插入节点中
	if parent.NumKeys < order-1 {
		insertIntoNode(parent, leftIndex+1, key, right)
		return
	}

	t.insertIntoNodeAfterSplitting(parent, leftIndex+1, key, right)
}

// insertIntoNewRoot 在left、right节点父节点为nil时，创建新父节点
func (t *Tree) insertIntoNewRoot(left *Node, right *Node) {
	t.Root = makeNode()

	t.Root.Keys[0] = left.Keys[left.NumKeys-1]
	t.Root.Keys[1] = right.Keys[right.NumKeys-1]

	t.Root.Pointers[0] = left
	t.Root.Pointers[1] = right
	t.Root.NumKeys = 2

	t.Root.Parent = nil
	left.Parent = t.Root
	right.Parent = t.Root
}

// getNodeIndex 找到node所在位置。若找不到就返回pointers最后一位索引
func getNodeIndex(parent, node *Node) int {
	var i int
	for i < parent.NumKeys && parent.Pointers[i] != node {
		i++
	}
	return i
}

// insertIntoNode 插入右节点
func insertIntoNode(parent *Node, rightIndex, key int, right *Node) {
	// 将插入节点位置右边节点右移一位
	for i := parent.NumKeys; i > rightIndex; i-- {
		parent.Pointers[i] = parent.Pointers[i-1]
		parent.Keys[i] = parent.Keys[i-1]
	}
	parent.Pointers[rightIndex] = right
	parent.Keys[rightIndex] = key
	parent.NumKeys++
}

// insertIntoNodeAfterSplitting 在分裂之后插入新节点
func (t *Tree) insertIntoNodeAfterSplitting(oldNode *Node, rightIndex, key int, right *Node) {
	values := make([]interface{}, order)
	keys := make([]int, order)

	// 将旧节点value、key复制到临时keys、values
	var j int
	for i := 0; i < oldNode.NumKeys; i++ {
		// TODO 为什么是leftIndex+1呢？ 因为是要找到key后边的那个节点。node将key夹在中间了
		if j == rightIndex {
			j++
		}
		values[j] = oldNode.Pointers[i]
		j++
	}
	j = 0
	for i := 0; i < oldNode.NumKeys; i++ {
		if j == rightIndex {
			j++
		}
		keys[j] = oldNode.Keys[i]
		j++
	}
	values[rightIndex] = right
	keys[rightIndex] = key

	// 将临时keys、values前半部分移到旧节点前半部分，后半部分移到新节点前半部分
	// TODO 为什么没有将oldNode后半部分置为nil呢？只要有numKeys标记就可以了，为什么还要置为nil呢
	newNode := makeNode()
	split := cut(order)
	oldNode.NumKeys = 0
	var valuesIndex int
	for valuesIndex = 0; valuesIndex < split; valuesIndex++ {
		oldNode.Pointers[valuesIndex] = values[valuesIndex]
		oldNode.Keys[valuesIndex] = keys[valuesIndex]
		oldNode.NumKeys++
	}
	var l int
	for l = 0; valuesIndex < order; valuesIndex++ {
		newNode.Pointers[l] = values[valuesIndex]
		newNode.Keys[l] = keys[valuesIndex]
		newNode.NumKeys++
		l++
	}
	newNode.Parent = oldNode.Parent

	// 使得新节点的子节点parent都为新节点
	// TODO <= node.NumKeys. 怎么想也不是很对劲啊！ 对于非叶节点Node的数量本来就是numKeys+1
	for i := 0; i < newNode.NumKeys; i++ {
		newNode.Pointers[i].(*Node).Parent = newNode
	}

	// 插入新节点到parent
	t.insertIntoParent(oldNode, newNode.Keys[newNode.NumKeys-1], newNode)
}

func (t *Tree) createNewTree(key int, r *Record) {
	t.Root = makeLeaf()
	t.Root.Keys[0] = key
	t.Root.Pointers[0] = r
	t.Root.NumKeys += 1
}

func makeLeaf() *Node {
	l := makeNode()
	l.IsLeaf = true
	return l
}

func makeNode() *Node {
	return &Node{
		Pointers: make([]interface{}, order+1),
		Keys:     make([]int, order+1),
		Parent:   nil,
		IsLeaf:   false,
		NumKeys:  0,
		Next:     nil,
	}
}

type metaAlter func(meta *RecordMeta) *RecordMeta

// UpdateRecord 更新数据特定字段
func (t *Tree) UpdateRecord(updatedData map[int]string, key int, ma metaAlter) error {
	record, err := t.Find(key)
	if err != nil {
		return err
	}
	if record.deleted {
		return ErrKeyNotFound
	}

	// 创建锁或者获取锁
	// TODO 如果没有可重入锁就跟无法锁一定不为nil，只要加锁存在缝隙，就会导致为nil
	// 还是先不设置为nil吧
	// TODO 像这种先rlock，然后lock都是有问题的。rlock谁都能得到，可是一旦要求lock的时候谁都得不到了
	t.recordLock.Lock()
	if record.lock == nil {
		record.lock = &sync.Mutex{}
	}
	record.lock.Lock()
	defer record.lock.Unlock()
	t.recordLock.Unlock()

	if record.deleted {
		return ErrKeyNotFound
	}

	meta := record.Meta
	if ma != nil {
		meta = ma(record.Meta)
	}
	// 更新前处理数据
	if t.inspector != nil {
		// TODO 如何传入txID给inspector呢？
		var nrm *RecordMeta
		if ma != nil {
			nrm = meta
		}
		t.inspector.HandleDataBeforeUpdate(record, nrm)
	}

	// 更新数据
	sameValueUpdate := 0
	for index, value := range updatedData {
		if record.Value[index] == value {
			sameValueUpdate++
			continue
		}
		record.Value[index] = value
	}
	if sameValueUpdate == len(updatedData) {
		return ErrUpdateSame
	}

	record.Meta = meta

	return nil
}

func (t *Tree) FineByValue(isTarget IsTarget) ([]*Record, error) {
	// 找到最左边的叶节点
	n := t.Root
	if n == nil {
		return nil, ErrValueNotFound
	}

	for !n.IsLeaf {
		n = n.Pointers[0].(*Node)
	}

	// 遍历叶节点，记录所有符合条件的记录
	rs := make([]*Record, 0)
	var r *Record
	ok := true
	for ok {
		for i := 0; i < n.NumKeys; i++ {
			r = n.Pointers[i].(*Record)
			if isTarget(r) {
				rs = append(rs, r)
			}
		}
		if n.Pointers[order-1] == nil {
			break
		}
		n, ok = n.Pointers[order-1].(*Node)
	}

	if len(rs) == 0 {
		return nil, ErrValueNotFound
	}
	return rs, nil
}

func (t *Tree) Find(key int) (*Record, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	// 找到key所在叶节点
	l := t.findLeaf(key)
	if l == nil {
		return nil, ErrKeyNotFound
	}

	// 找到key所在叶节点位置
	var i int
	for i = 0; i < l.NumKeys; i++ {
		if l.Keys[i] == key {
			break
		}
	}
	if i == l.NumKeys {
		return nil, ErrKeyNotFound
	}

	return l.Pointers[i].(*Record), nil
}

func (t *Tree) findLeaf(key int) *Node {
	// 空树返回nil
	n := t.Root
	if n == nil {
		return nil
	}

	var i int
	// 找到key对应叶节点
	for !n.IsLeaf {
		i = 0
		// 找到key对应node。就是恰好不小于key的node
		// 在key、pointer相等的情况下，若key大于所有keys那么只能是最后一个
		// 既然大于所有keys只能是最后一个，那么key应该为keys中最后一个
		for i < n.NumKeys-1 {
			if key > n.Keys[i] {
				i += 1
			} else {
				break
			}
		}
		// 一直发生看上去被删的key却被当作索引的原因在于最后赋予parentNode的keys可能出现了问题
		// child key明明没了，可是在parent那里却仍然存在
		n = n.Pointers[i].(*Node)
	}

	return n
}

func cut(l int) int {
	if l%2 == 0 {
		return l / 2
	}
	return l/2 + 1
}

func (t *Tree) Delete(key int) error {
	record, err := t.Find(key)
	if err != nil {
		return err
	}
	if record.deleted {
		return ErrKeyNotFound
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	t.recordLock.Lock()
	if record.lock != nil {
		record.lock.Lock()
		defer record.lock.Unlock()
	}
	t.recordLock.Unlock()
	if record.deleted {
		return ErrKeyNotFound
	}

	// 删除前处理数据
	if t.inspector != nil {
		t.inspector.HandleDataBeforeDelete(record)
	}

	leaf := t.findLeaf(key)
	if record != nil && leaf != nil {
		err = t.deleteEntry(leaf, key, record)
		if err != nil {
			if err == ErrNoSuchChild {
				return ErrKeyNotFound
			}
			return err
		}
	}

	record.deleted = true

	return nil
}

func (t *Tree) deleteEntry(node *Node, key int, p interface{}) error {
	originNKey := node.Keys[node.NumKeys-1]
	// 从node删除该条目，并返回该node
	var err error
	if node.IsLeaf {
		node, err = removeEntryFromLeaf(node, key)
	} else {
		node, err = removeEntryFromNode(node, p)
	}
	if err != nil {
		return err
	}

	// 若删除条目的node为root，就调节一下root
	if node == t.Root {
		t.adjustRoot()
		return nil
	}

	// 当节点大于等于要求的最小数量时就直接返回
	if node.NumKeys >= cut(order-1) {
		return nil
	}

	// 若parent只有一个节点，那么也不需要迁移合并了
	if node.Parent.NumKeys == 1 {
		return nil
	}

	// 找到邻居节点。若节点是0节点，那么邻居就是1节点；其他情况，邻居为左节点
	neighbourIndex := getNeighbourIndex(node)
	if neighbourIndex >= node.Parent.NumKeys {
		return ErrNoSuchChild
	}
	var neighbour *Node
	if neighbourIndex == -1 {
		// TODO 能确保node是零节点的时候，一定存在一节点吗？不能保证
		neighbour, _ = node.Parent.Pointers[1].(*Node)
	} else {
		// 为何此处neighbourIndex超出数量限制呢？除非在父节点根本找不到
		neighbour, _ = node.Parent.Pointers[neighbourIndex].(*Node)
	}

	var kPrimeIndex int
	if neighbourIndex == -1 {
		kPrimeIndex = 0
	} else {
		kPrimeIndex = neighbourIndex
	}

	if neighbour.NumKeys+node.NumKeys < order {
		// 当邻居节点数量与节点数量总和小于容量时，就合并节点
		return t.coalesceNodes(node, neighbour, neighbourIndex, originNKey)
	} else {
		// 当邻居节点数量与节点数量总和大于容量时，就迁移node第一个节点到邻居节点
		t.redistributeNodes(node, neighbour, neighbourIndex, kPrimeIndex)
	}

	return nil
}

func removeEntryFromLeaf(n *Node, key int) (*Node, error) {
	// 找到key对应位置
	// TODO index out of range 2 with len 2
	// 我第一个猜想是findLeaf并没有找到leaf。居然没有发现findLeaf找到非leaf的情况
	// coalesceNodes 传入的节点并非叶节点，里面的方法却当叶节点在用
	var delPoint int
	for n.Keys[delPoint] != key {
		delPoint++
	}
	if delPoint >= n.NumKeys {
		return nil, ErrNoSuchChild
	}

	// 删除该key
	for i := delPoint + 1; i < n.NumKeys; i++ {
		n.Keys[i-1] = n.Keys[i]
	}

	// 删除pointer
	for i := delPoint + 1; i < n.NumKeys; i++ {
		n.Pointers[i-1] = n.Pointers[i]
	}
	n.NumKeys--

	return n, nil
}

func removeEntryFromNode(n *Node, p interface{}) (*Node, error) {
	var nodeIndex int
	for ; nodeIndex < n.NumKeys; nodeIndex++ {
		if reflect.DeepEqual(n.Pointers[nodeIndex], p) {
			break
		}
	}
	if nodeIndex >= n.NumKeys {
		// no such child
		return nil, ErrNoSuchChild
	}

	// 删除对应子节点
	for j := nodeIndex + 1; j < n.NumKeys; j++ {
		n.Pointers[j-1] = n.Pointers[j]
	}

	// 删除对应key。
	// 那是不是key向前移，替换到i-1的key呢？
	for i := nodeIndex + 1; i < n.NumKeys; i++ {
		n.Keys[i-1] = n.Keys[i]
	}
	n.NumKeys--

	return n, nil
}

// adjustRoot 当从root删除数据，就调节root节点
func (t *Tree) adjustRoot() {
	// root还有key就不用调节
	if t.Root.NumKeys > 0 {
		return
	}

	t.Root = nil
}

// getNeighbourIndex 找到节点的左节点位置
func getNeighbourIndex(n *Node) int {
	var i int
	for i = 0; i <= n.Parent.NumKeys; i++ {
		if reflect.DeepEqual(n.Parent.Pointers[i], n) {
			return i - 1
		}
	}
	return i
}

func (t *Tree) coalesceNodes(n, neighbour *Node, neighbourIndex, originalNKey int) error {
	// 当节点为最左节点时，交换节点和邻居节点。此后，节点为原节点1
	if neighbourIndex == -1 {
		n, neighbour = neighbour, n
		originalNKey = n.Keys[n.NumKeys-1]
	}

	// TODO 这里居然index也会为-1
	//originalNKey := n.Keys[n.NumKeys-1]

	var j int
	i := neighbour.NumKeys
	for j = 0; j < n.NumKeys; j++ {
		neighbour.Keys[i] = n.Keys[j]
		neighbour.Pointers[i] = n.Pointers[j]
		neighbour.NumKeys++
		n.NumKeys--
		i++
	}

	if n.IsLeaf {
		neighbour.Pointers[order] = n.Pointers[order]
	} else {
		for k := 0; k < neighbour.NumKeys; k++ {
			neighbour.Pointers[i].(*Node).Parent = neighbour
		}
	}

	// 从父节点中删去n节点
	// TODO 若n节点全被删了的话，那么可能就无法从parent删除了。所以key应该为原来的
	return t.deleteEntry(n.Parent, originalNKey, n)
}

// redistributeNodes 移动节点一值到邻居节点
func (t *Tree) redistributeNodes(n, neighbor *Node, neighbourIndex, neighborIndexInParent int) {
	if neighbourIndex == -1 {
		n, neighbor = neighbor, n
	}
	if neighbor.NumKeys >= order-1 {
		return
	}

	neighbor.Keys[neighbor.NumKeys] = n.Keys[0]
	neighbor.Pointers[neighbor.NumKeys] = n.Pointers[0]
	neighbor.Parent.Keys[neighborIndexInParent] = n.Keys[0]

	var i int
	for i = 0; i < n.NumKeys; i++ {
		n.Keys[i] = n.Keys[i+1]
		n.Pointers[i] = n.Pointers[i+1]
	}

	n.NumKeys--
	neighbor.NumKeys++
}
