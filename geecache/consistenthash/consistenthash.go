package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// 定义函数类型 HashFunc，采取依赖注入的方式，允许用于替换成自定义的 HashFunc 函数
// 默认为 crc32.ChecksumIEEE 算法
type HashFunc func(data []byte) uint32

// Map constains all hashed keys
type Map struct {
	hashfunc HashFunc
	replicas int            // 虚拟节点倍数
	keys     []int          // 哈希环 Sorted
	hashMap  map[int]string // 虚拟节点与真实节点的映射表，键是虚拟节点的哈希值，值是真实节点的名称
}

// 允许自定义虚拟节点倍数和 Hash 函数
func New(replicas int, hashfunc HashFunc) *Map {
	m := &Map{
		replicas: replicas,
		hashfunc: hashfunc,
		hashMap:  make(map[int]string),
	}
	if m.hashfunc == nil {
		m.hashfunc = crc32.ChecksumIEEE
	}
	return m
}

// 允许传入 0 或 多个真实节点的名称
func (m *Map) Add(keys ...string) {
	// 对每一个真实节点 key，对应创建 m.replicas 个虚拟节点
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ {
			// 虚拟节点的名称是：strconv.Itoa(i) + key
			// m.hashfunc() 计算虚拟节点的哈希值
			hashvalue := int(m.hashfunc([]byte(strconv.Itoa(i) + key)))
			// 添加到哈希环上
			m.keys = append(m.keys, hashvalue)
			// 增加虚拟节点和真实节点的映射关系
			m.hashMap[hashvalue] = key
		}
	}
	// 环上的哈希值排序
	sort.Ints(m.keys)
}

// Get gets the closest item in the hashfunc to the provided key.
func (m *Map) Get(key string) string {
	if len(m.keys) == 0 {
		return ""
	}
	hashvalue := int(m.hashfunc([]byte(key)))
	// 顺时针找到第一个匹配的虚拟节点的下标 idx
	idx := sort.Search(len(m.keys), func(i int) bool {
		// 获取到对应的哈希值，如果 idx == len(m.keys)，说明应选择 m.keys[0]
		return m.keys[i] >= hashvalue
	})
	// 因为 m.keys 是一个环状结构，所以用取余处理，映射得到真实的节点
	return m.hashMap[m.keys[idx%len(m.keys)]]
}
