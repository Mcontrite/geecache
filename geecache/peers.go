package geecache

import pb "geecache/geecachepb"

// 根据传入的 key 选择相应 PeerGetter 节点
type PeerPicker interface {
	PickPeer(key string) (peer PeerGetter, ok bool)
}

// 对应 HTTP 客户端，从 group 查找缓存值
type PeerGetter interface {
	Get(in *pb.Request, out *pb.Response) error
}
