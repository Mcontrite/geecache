package singleflight

import "sync"

// 正在进行中，或已经结束的请求
type call struct {
	wg  sync.WaitGroup // 并发协程之间不需要消息传递，非常适合 sync.WaitGroup
	val interface{}
	err error
}

// 管理不同 key 的请求(call)
// Group represents a class of work and forms a namespace in which
// units of work can be executed with duplicate suppression.
type Group struct {
	mu sync.Mutex       // 保护成员变量 m 不被并发读写
	m  map[string]*call // lazily initialized
}

// 针对相同的 key，无论 Do 被调用多少次，函数 fn 都只被调用一次
// 等待 fn 调用结束了，返回返回值或错误
func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()         // 如果请求正在进行中，则等待
		return c.val, c.err // 请求结束，返回结果
	}
	c := new(call)
	c.wg.Add(1)  // 发起请求前加锁
	g.m[key] = c // 添加到 g.m，表明 key 已经有对应的请求在处理
	g.mu.Unlock()

	c.val, c.err = fn() // 调用 fn，发起请求
	c.wg.Done()         // 请求结束

	g.mu.Lock()
	delete(g.m, key) // 更新 g.m
	g.mu.Unlock()

	return c.val, c.err // 返回结果
}
