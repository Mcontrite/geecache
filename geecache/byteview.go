package geecache

// 表示缓存值
type ByteView struct {
	b []byte // 存储真实的缓存值，能支持任意数据类型的存储，例如字符串、图片等
}

// Len returns the view's length
func (v ByteView) Len() int {
	return len(v.b)
}

// b 是只读的，ByteSlice() 方法返回一个拷贝，防止缓存值被外部程序修改
func (v ByteView) ByteSlice() []byte {
	return cloneBytes(v.b)
}

func cloneBytes(b []byte) []byte {
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// String returns the data as a string, making a copy if necessary.
func (v ByteView) String() string {
	return string(v.b)
}
