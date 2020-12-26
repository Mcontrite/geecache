package geecache

import (
	"fmt"
	pb "geecache/geecachepb"
	"geecache/singleflight"
	"log"
	"sync"
)

// 负责与用户的交互，控制缓存值存储和获取的流程
// 可以认为是一个缓存的命名空间，每个 Group 拥有唯一的名称
type Group struct {
	name      string
	callback  Callback // 缓存未命中时获取源数据的回调
	lockCache LockCache
	peers     PeerPicker
	loader    *singleflight.Group // 确保每个缓存键只查询一次
}

// 回调函数(callback)，在缓存不存在时，调用这个函数，得到源数据
type Callback interface {
	CallbackGet(key string) ([]byte, error)
}

// 定义函数类型
type CallbackFunc func(key string) ([]byte, error)

// 实现 Callback 接口
func (f CallbackFunc) CallbackGet(key string) ([]byte, error) {
	return f(key)
}

var (
	mu     sync.RWMutex
	groups = make(map[string]*Group)
)

// 实例化 Group，并将对象保存到全局变量 groups
func NewGroup(name string, cacheBytes int64, callback Callback) *Group {
	if callback == nil {
		panic("nil Callback")
	}
	mu.Lock()
	defer mu.Unlock()
	g := &Group{
		name:      name,
		callback:  callback,
		lockCache: LockCache{cacheBytes: cacheBytes},
		loader:    &singleflight.Group{},
	}
	// 将 group 存储在全局变量 groups 中
	groups[name] = g
	return g
}

// 获取指定名称的 Group，因为不涉及任何冲突变量的写操作，所以使用只读锁 RLock()
func GetGroup(name string) *Group {
	mu.RLock()
	g := groups[name]
	mu.RUnlock()
	return g
}

// Get value for a key from cache
func (g *Group) Get(key string) (ByteView, error) {
	if key == "" {
		return ByteView{}, fmt.Errorf("key is required")
	}
	// 从 lockCache 中查找缓存，如果存在则返回缓存值
	if v, ok := g.lockCache.get(key); ok {
		log.Println("[GeeCache] hit")
		return v, nil
	}
	return g.load(key)
}

func (g *Group) load(key string) (value ByteView, err error) {
	// each key is only fetched once (either locally or remotely)
	// regardless of the number of concurrent callers.
	viewi, err := g.loader.Do(key, func() (interface{}, error) {
		if g.peers != nil {
			// PickPeer() 方法选择节点
			if peer, ok := g.peers.PickPeer(key); ok {
				// 若非本机节点则调用 getFromPeer() 从远程获取
				if value, err = g.getFromPeer(peer, key); err == nil {
					return value, nil
				}
				log.Println("[GeeCache] Failed to get from peer", err)
			}
		}
		// 若是本机节点或失败，则回退到 getLocally()
		return g.getLocally(key)
	})
	if err == nil {
		return viewi.(ByteView), nil
	}
	return
}

func (g *Group) getLocally(key string) (ByteView, error) {
	// 调用用户回调函数获取源数据
	bytes, err := g.callback.CallbackGet(key)
	if err != nil {
		return ByteView{}, err

	}
	value := ByteView{b: cloneBytes(bytes)}
	g.populateCache(key, value)
	return value, nil
}

// 将源数据添加到缓存 lockCache 中
func (g *Group) populateCache(key string, value ByteView) {
	g.lockCache.add(key, value)
}

// 将实现了 PeerPicker 接口的 HTTPServer 注入到 Group 中
func (g *Group) RegisterPeers(peers PeerPicker) {
	if g.peers != nil {
		panic("RegisterPeerPicker called more than once")
	}
	g.peers = peers
}

// 从其他节点获取
func (g *Group) getFromPeer(peer PeerGetter, key string) (ByteView, error) {
	// 使用实现了 PeerGetter 接口的 httpClient 访问远程节点，获取缓存值
	req := &pb.Request{
		Group: g.name,
		Key:   key,
	}
	res := &pb.Response{}
	err := peer.Get(req, res)
	if err != nil {
		return ByteView{}, err
	}
	return ByteView{b: res.Value}, nil
}
