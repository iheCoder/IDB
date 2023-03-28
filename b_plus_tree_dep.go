package IDB

import "sync"

type BPItemDepred struct {
	key int64
	val interface{}
}

type BPNodeDep struct {
	// 储存子树的最大key
	MaxKey int64
	// 节点子树
	Nodes []*BPNodeDep
	// 叶子节点记录的数据记录
	Items []*BPItemDepred
	// 叶子结点指向下一个叶节点
	Next *BPNodeDep
}

type BPTreeDep struct {
	mu   *sync.RWMutex
	root *BPNodeDep
	// 表示B+树阶
	width int
	//
	halfW int
}

func NewBPTreeDep(width int) *BPTreeDep {
	if width < 3 {
		width = 3
	}
	return &BPTreeDep{
		mu:    &sync.RWMutex{},
		root:  NewLeafNodeDep(width),
		width: width,
		halfW: (width + 1) / 2,
	}
}

func NewLeafNodeDep(width int) *BPNodeDep {
	return &BPNodeDep{
		Items: make([]*BPItemDepred, 0, width+1),
	}
}

func NewIndexNodeDep(width int) *BPNodeDep {
	return &BPNodeDep{
		Nodes: make([]*BPNodeDep, 0, width+1),
	}
}

// findItem 找到叶节点对应key的位置
func (n *BPNodeDep) findItem(key int64) int {
	for i := 0; i < len(n.Items); i++ {
		if n.Items[i].key > key {
			return -1
		} else if n.Items[i].key == key {
			return i
		}
	}
	return -1
}

// setValue 将元素添加到叶节点
func (n *BPNodeDep) setValue(key int64, value interface{}) {
	item := &BPItemDepred{
		key: key,
		val: value,
	}

	if len(n.Items) == 0 || key > n.Items[len(n.Items)-1].key { // 若items为空或者要添加key大于最大的key，直接添加到最后并更新maxKey
		n.Items = append(n.Items, item)
		n.MaxKey = item.key
		return
	} else if key < n.Items[0].key { // 若要添加的key小于最小的key，直接插入items首
		n.Items = append([]*BPItemDepred{item}, n.Items...)
		return
	}

	for i := 0; i < len(n.Items); i++ {
		if n.Items[i].key > key { // 找到首个大于要添加key时就添加进去
			n.Items = append(n.Items, &BPItemDepred{})
			copy(n.Items[i+1:], n.Items[i:])
			n.Items[i] = item
			return
		} else if n.Items[i].key == key { // 若找到相等key，就直接替代
			n.Items[i] = item
			return
		}
	}
}

// addChild 添加子节点
func (n *BPNodeDep) addChild(child *BPNodeDep) {
	if len(n.Nodes) < 1 || child.MaxKey > n.Nodes[len(n.Nodes)-1].MaxKey {
		n.Nodes = append(n.Nodes, child)
		n.MaxKey = child.MaxKey
		return
	} else if child.MaxKey < n.Nodes[0].MaxKey {
		n.Nodes = append([]*BPNodeDep{child}, n.Nodes...)
		return
	}

	for i := 0; i < len(n.Nodes); i++ {
		if n.Nodes[len(n.Nodes)-1].MaxKey > child.MaxKey {
			n.Nodes = append(n.Nodes, &BPNodeDep{})
			copy(n.Nodes[i+1:], n.Nodes[i:])
			n.Nodes[i] = child
			return
		}
	}
}

// deleteItem 叶节点删除元素
func (n *BPNodeDep) deleteItem(key int64) bool {
	num := len(n.Items)
	for i := 0; i < num; i++ {
		if n.Items[i].key > key { // 当找到大于key时就不可能存在了
			return false
		} else if n.Items[i].key == key { // 当找到相等的时候，就直接删除并更新maxKey
			copy(n.Items[i:], n.Items[i+1:])
			n.Items = n.Items[:len(n.Items)-1]
			// 这点他确实没有考虑到，如果都为空了，那么maxKey哪里来呢
			if len(n.Items) == 0 {
				n.MaxKey = 0
			} else {
				n.MaxKey = n.Items[len(n.Items)-1].key
			}
			return true
		}
	}
	return false
}

// deleteChild 删除子节点
func (n *BPNodeDep) deleteChild(child *BPNodeDep) bool {
	num := len(n.Nodes)
	for i := 0; i < num; i++ {
		if n.Nodes[i] == child {
			copy(n.Nodes[i:], n.Nodes[i+1:])
			n.Nodes = n.Nodes[0 : len(n.Nodes)-1]
			n.MaxKey = n.Nodes[len(n.Nodes)-1].MaxKey
			return true
		}
	}
	return false
}

func (t *BPTreeDep) Get(key int64) interface{} {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 递归遍历，直到找到对应节点
	node := t.root
	for i := 0; i < len(node.Nodes); i++ {
		if key <= node.Nodes[i].MaxKey {
			node = node.Nodes[i]
			i = -1
		}
	}

	// 没有到达叶节点
	if len(node.Nodes) > 0 {
		return nil
	}

	// 在叶节点寻找到相应item
	for i := 0; i < len(node.Items); i++ {
		if node.Items[i].key == key {
			return node.Items[i].val
		}
	}

	return nil
}

// splitNode 从原node中分裂新halfW到新node或者分裂item到新item
func (t *BPTreeDep) splitNode(node *BPNodeDep) *BPNodeDep {
	// 分离Node
	if len(node.Nodes) > t.width {
		halfW := t.width/2 + 1
		n := NewIndexNodeDep(t.width)
		n.Nodes = append(n.Nodes, node.Nodes[halfW:len(node.Nodes)]...)
		n.MaxKey = n.Nodes[len(n.Nodes)-1].MaxKey

		node.Nodes = node.Nodes[:halfW]
		node.MaxKey = node.Nodes[len(node.Nodes)-1].MaxKey
		return n
	} else if len(node.Items) > t.width {
		//创建新结点
		halfw := t.width/2 + 1
		n := NewLeafNodeDep(t.width)
		n.Items = append(n.Items, node.Items[halfw:len(node.Items)]...)
		n.MaxKey = n.Items[len(n.Items)-1].key

		//修改原结点数据
		node.Next = n
		node.Items = node.Items[0:halfw]
		node.MaxKey = node.Items[len(node.Items)-1].key

		return n
	}
	return nil
}

func (t *BPTreeDep) setValue(parent *BPNodeDep, node *BPNodeDep, key int64, value interface{}) {
	// 递归遍历，找到设置Node
	for i := 0; i < len(node.Nodes); i++ {
		if key <= node.Nodes[i].MaxKey || i == len(node.Nodes)-1 {
			t.setValue(node, node.Nodes[i], key, value)
			break
		}
	}

	// 若是叶节点就添加数据
	if len(node.Nodes) < 1 {
		node.setValue(key, value)
	}

	// 尝试节点分裂
	newNode := t.splitNode(node)
	if newNode != nil {
		// 只有node为nil的情况，parent才会为nil。因此设置该树的root为新创建的父节点，且父节点设置node以及新node为子节点
		if parent == nil {
			parent = NewIndexNodeDep(t.width)
			parent.addChild(node)
			t.root = parent
		}
		parent.addChild(newNode)
	}
}

func (t *BPTreeDep) Set(key int64, value interface{}) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.setValue(nil, t.root, key, value)
}

// itemMoveOrMerge 叶节点合并
func (t *BPTreeDep) itemMoveOrMerge(parent *BPNodeDep, node *BPNodeDep) {
	var nl, nr *BPNodeDep
	// 当parent为nil的时候就会panic掉。我甚至不知道这该怎么改！
	for i := 0; i < len(parent.Nodes); i++ {
		if parent.Nodes[i] == node {
			if i < len(parent.Nodes)-1 { // 当node不是最后节点时
				nr = parent.Nodes[i+1]
			} else if i > 0 { // 当node是最后节点且大于总节点数大于1时
				nl = parent.Nodes[i-1]
			}
			break
		}
	}

	// 将node左节点最后item移到node最前面
	// 我不明白为什么是将左节点最后一个item移到node前面，这合并有点怪啊？还是说一个一个地合并啊
	if nl != nil && len(nl.Items) > t.halfW {
		item := nl.Items[len(nl.Items)-1]
		nl.Items = nl.Items[:len(nl.Items)-1]
		nl.MaxKey = nl.Items[len(nl.Items)-1].key
		node.Items = append([]*BPItemDepred{item}, node.Items...)
		return
	}

	// 当node不是最后节点时，将node右节点第一个item移到node最后
	if nr != nil && len(nr.Items) > t.halfW {
		item := nr.Items[0]
		nr.Items = nl.Items[1:]
		node.Items = append(node.Items, item)
		node.MaxKey = node.Items[len(node.Items)-1].key
		return
	}

	// 与左节点合并
	if nl != nil && len(nl.Items)+len(node.Items) <= t.width {
		nl.Items = append(nl.Items, node.Items...)
		nl.Next = node.Next
		nl.MaxKey = nl.Items[len(nl.Items)-1].key
		parent.deleteChild(node)
		return
	}

	// 与右节点合并
	if nr != nil && len(nr.Items)+len(node.Items) <= t.width {
		node.Items = append(node.Items, nr.Items...)
		node.Next = nr.Next
		node.MaxKey = node.Items[len(node.Items)-1].key
		parent.deleteChild(nr)
		return
	}
}

func (t *BPTreeDep) childMoveOrMerge(parent *BPNodeDep, node *BPNodeDep) {
	if parent == nil {
		return
	}

	//获取兄弟结点
	var nl *BPNodeDep = nil
	var nr *BPNodeDep = nil
	for i := 0; i < len(parent.Nodes); i++ {
		if parent.Nodes[i] == node {
			if i < len(parent.Nodes)-1 {
				nr = parent.Nodes[i+1]
			} else if i > 0 {
				nl = parent.Nodes[i-1]
			}
			break
		}
	}

	//将左侧结点的子结点移动到删除结点
	if nl != nil && len(nl.Nodes) > t.halfW {
		n := nl.Nodes[len(nl.Nodes)-1]
		nl.Nodes = nl.Nodes[0 : len(nl.Nodes)-1]
		node.Nodes = append([]*BPNodeDep{n}, node.Nodes...)
		return
	}

	//将右侧结点的子结点移动到删除结点
	if nr != nil && len(nr.Nodes) > t.halfW {
		n := nr.Nodes[0]
		nr.Nodes = nl.Nodes[1:]
		node.Nodes = append(node.Nodes, n)
		return
	}

	if nl != nil && len(nl.Nodes)+len(node.Nodes) <= t.width {
		nl.Nodes = append(nl.Nodes, node.Nodes...)
		parent.deleteChild(node)
		return
	}

	if nr != nil && len(nr.Nodes)+len(node.Nodes) <= t.width {
		node.Nodes = append(node.Nodes, nr.Nodes...)
		parent.deleteChild(nr)
		return
	}
}

func (t *BPTreeDep) deleteItem(parent *BPNodeDep, node *BPNodeDep, key int64) {
	// 找到item所在node去删除
	for i := 0; i < len(node.Nodes); i++ {
		if key <= node.Nodes[i].MaxKey {
			t.deleteItem(node, node.Nodes[i], key)
			break
		}
	}

	// 当找到的node为叶节点，就从node中删除该key，并在items数量小于halfW时，合并
	if len(node.Nodes) == 0 {
		node.deleteItem(key)
		if len(node.Items) < t.halfW {
			t.itemMoveOrMerge(parent, node)
		}
		return
	}

	node.MaxKey = node.Nodes[len(node.Nodes)-1].MaxKey
	if len(node.Nodes) < t.halfW {
		t.childMoveOrMerge(parent, node)
	}
}

func (t *BPTreeDep) Remove(key int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.deleteItem(nil, t.root, key)
}
