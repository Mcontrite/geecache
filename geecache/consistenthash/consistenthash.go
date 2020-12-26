package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// ConsistHash constains all hashed keys
type ConsistHash struct {
	hashfunc HashFunc       // 哈希函数
	replicas int            // 虚拟节点倍数
	keys     []int          // 哈希环 Sorted
	hashMap  map[int]string // 虚拟节点与真实节点的映射表，键是虚拟节点的哈希值，值是真实节点的名称
}

// 定义函数类型 HashFunc，采取依赖注入的方式，允许用于替换成自定义的 HashFunc 函数
// 默认为 crc32.ChecksumIEEE 算法
type HashFunc func(data []byte) uint32

// 允许自定义虚拟节点倍数和 Hash 函数
func New(replicas int, hashfunc HashFunc) *ConsistHash {
	h := &ConsistHash{
		replicas: replicas,
		hashfunc: hashfunc,
		hashMap:  make(map[int]string),
	}
	if h.hashfunc == nil {
		h.hashfunc = crc32.ChecksumIEEE
	}
	return h
}

// 允许传入 0 或 多个真实节点的名称
func (h *ConsistHash) Add(keys ...string) {
	// 每一个真实节点 key 对应创建 h.replicas 个虚拟节点
	for _, key := range keys {
		for i := 0; i < h.replicas; i++ {
			// 虚拟节点的名称是 strconv.Itoa(i) + key，添加编号以区分不同虚拟节点
			// h.hashfunc() 计算虚拟节点的哈希值
			hashvalue := int(h.hashfunc([]byte(strconv.Itoa(i) + key)))
			// 添加到哈希环上
			h.keys = append(h.keys, hashvalue)
			// 增加虚拟节点和真实节点的映射关系
			h.hashMap[hashvalue] = key
		}
	}
	// 环上的哈希值排序
	sort.Ints(h.keys)
}

// Get gets the closest item in the hashfunc to the provided key.
func (h *ConsistHash) Get(key string) string {
	if len(h.keys) == 0 {
		return ""
	}
	// 根据 key 计算哈希值
	hashvalue := int(h.hashfunc([]byte(key)))
	// 顺时针找到第一个匹配的虚拟节点的下标 idx
	idx := sort.Search(len(h.keys), func(i int) bool {
		// 从 keys 中获取对应的哈希值，如果 idx == len(h.keys)，说明应选择 h.keys[0]
		return h.keys[i] >= hashvalue
	})
	// 因为 h.keys 是一个环状结构，所以用取余处理，映射得到真实的节点
	return h.hashMap[h.keys[idx%len(h.keys)]]
}

// Remove use to remove a key and its virtual keys on the ring and map
func (h *ConsistHash) Remove(key string) {
	for i := 0; i < h.replicas; i++ {
		hash := int(h.hashfunc([]byte(strconv.Itoa(i) + key)))
		idx := sort.SearchInts(h.keys, hash)
		h.keys = append(h.keys[:idx], h.keys[idx+1:]...)
		delete(h.hashMap, hash)
	}
}
