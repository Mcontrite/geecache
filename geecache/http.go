package geecache

import (
	"fmt"
	"geecache/consistenthash"
	pb "geecache/geecachepb"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
)

const (
	defaultBasePath = "/_geecache/"
	defaultReplicas = 50
)

// 提供 HTTP 服务，可以被其他节点访问
type HTTPServer struct {
	self        string                      // 记录自己的地址，包括主机名/IP 和端口，例如："https://example.net:8000"
	basePath    string                      // 节点间通讯地址的前缀，默认是 /_geecache/
	mu          sync.Mutex                  // guards peers and httpClients
	peers       *consistenthash.ConsistHash // 用来根据具体的 key 选择节点
	httpClients map[string]*HTTPClient      // 每个远程节点对应一个 HTTPClient，与远程节点的地址 baseURL 有关
}

// NewHTTPServer initializes an HTTP pool of peers.
func NewHTTPServer(self string) *HTTPServer {
	return &HTTPServer{
		self:     self,
		basePath: defaultBasePath,
	}
}

// Log info with server name
func (p *HTTPServer) Log(format string, v ...interface{}) {
	log.Printf("[Server %s] %s", p.self, fmt.Sprintf(format, v...))
}

// ServeHTTP handle all http requests
func (p *HTTPServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 判断访问路径的前缀是否是 basePath，不是则返回错误
	if !strings.HasPrefix(r.URL.Path, p.basePath) {
		panic("HTTPServer serving unexpected path: " + r.URL.Path)
	}
	p.Log("%s %s", r.Method, r.URL.Path)
	// 约定访问路径格式为 /<basepath>/<groupname>/<key>
	parts := strings.SplitN(r.URL.Path[len(p.basePath):], "/", 2)
	if len(parts) != 2 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	groupName := parts[0]
	key := parts[1]
	group := GetGroup(groupName)
	if group == nil {
		http.Error(w, "no such group: "+groupName, http.StatusNotFound)
		return
	}
	// 获取缓存数据
	view, err := group.Get(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// 编码 HTTP 响应
	body, err := proto.Marshal(&pb.Response{Value: view.ByteSlice()})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	// 将缓存值作为 httpResponse 的 body 返回
	w.Write(body)
}

// 实例化一致性哈希算法，添加传入的多个节点
func (p *HTTPServer) Set(peers ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers = consistenthash.New(defaultReplicas, nil)
	p.peers.Add(peers...)
	p.httpClients = make(map[string]*HTTPClient, len(peers))
	// 为每个节点创建了一个 HTTP 客户端 HTTPClient
	for _, peer := range peers {
		p.httpClients[peer] = &HTTPClient{baseURL: peer + p.basePath}
	}
}

// 包装了一致性哈希算法的 Get() 方法
// 根据具体的 key 选择节点，返回节点对应的 HTTP 客户端
func (p *HTTPServer) PickPeer(key string) (PeerGetter, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if peer := p.peers.Get(key); peer != "" && peer != p.self {
		p.Log("Pick peer %s", peer)
		return p.httpClients[peer], true
	}
	return nil, false
}

var _ PeerPicker = (*HTTPServer)(nil)

type HTTPClient struct {
	baseURL string // 将要访问的远程节点的地址
}

func (h *HTTPClient) Get(in *pb.Request, out *pb.Response) error {
	u := fmt.Sprintf(
		"%v%v/%v",
		h.baseURL,
		url.QueryEscape(in.GetGroup()),
		url.QueryEscape(in.GetKey()),
	)
	// 获取返回值
	res, err := http.Get(u)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("server returned: %v", res.Status)
	}
	// 转换为 []bytes 类型
	bytes, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("reading response body: %v", err)
	}
	// 解码 HTTP 响应
	if err = proto.Unmarshal(bytes, out); err != nil {
		return fmt.Errorf("decoding response body: %v", err)
	}
	return nil
}

var _ PeerGetter = (*HTTPClient)(nil)
