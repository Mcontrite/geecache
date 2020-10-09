package lru

import "container/list"

// Cache is a LRU cache. It is not safe for concurrent access.
type Cache struct {
	maxBytes       int64                         // 允许使用的最大内存
	nowBytes       int64                         // 当前已使用的内存
	doubleLinkList *list.List                    // 存储缓存值
	dictionary     map[string]*list.Element      // 存储缓存的键值映射关系
	OnEvicted      func(key string, value Value) // 记录被移除时的回调函数，可以为 nil。
}

// Len 返回值所占用的内存大小
type Value interface {
	Len() int
}

// 双向链表节点的数据类型，淘汰队首节点时需要用 key 从字典中删除对应的映射
type nodeData struct {
	key   string
	value Value // 允许值为实现 Value 接口的任意类型
}

// Len the number of cache entries
func (c *Cache) Len() int {
	return c.doubleLinkList.Len()
}

// New is the Constructor of Cache
func New(maxBytes int64, onEvicted func(string, Value)) *Cache {
	return &Cache{
		maxBytes:       maxBytes,
		doubleLinkList: list.New(),
		dictionary:     make(map[string]*list.Element),
		OnEvicted:      onEvicted,
	}
}

// Add adds a value to the cache.
func (c *Cache) Add(key string, value Value) {
	if ele, ok := c.dictionary[key]; ok {
		// 如果键存在则更新对应节点的值，并将该节点移到队尾
		c.doubleLinkList.MoveToFront(ele)
		kv := ele.Value.(*nodeData)
		c.nowBytes += int64(value.Len()) - int64(kv.value.Len())
		kv.value = value
	} else {
		// 不存在则新增，在队尾添加新节点
		ele := c.doubleLinkList.PushFront(&nodeData{key, value})
		// 在字典中添加 key 和节点的映射关系
		c.dictionary[key] = ele
		c.nowBytes += int64(len(key)) + int64(value.Len())
	}
	// 如果超过了设定的最大内存值，则移除最少访问的节点
	for c.maxBytes != 0 && c.maxBytes < c.nowBytes {
		c.RemoveOldest()
	}
}

// Get look ups a key's value
func (c *Cache) Get(key string) (value Value, ok bool) {
	if ele, ok := c.dictionary[key]; ok {
		// 将链表中的节点 ele 移动到队尾（双向链表作为队列，队首队尾是相对的，在这里约定 front 为队尾）
		c.doubleLinkList.MoveToFront(ele)
		kv := ele.Value.(*nodeData)
		return kv.value, true
	}
	return
}

// LRU 缓存淘汰，移除最近最少访问的节点（队首）
func (c *Cache) RemoveOldest() {
	// 取到队首节点
	ele := c.doubleLinkList.Back()
	if ele != nil {
		// 从链表中删除
		c.doubleLinkList.Remove(ele)
		kv := ele.Value.(*nodeData)
		// 从字典中删除节点的映射关系
		delete(c.dictionary, kv.key)
		// 更新当前所用的内存
		c.nowBytes -= int64(len(kv.key)) + int64(kv.value.Len())
		// 如果回调函数 OnEvicted 不为 nil，则调用回调函数
		if c.OnEvicted != nil {
			c.OnEvicted(kv.key, kv.value)
		}
	}
}
