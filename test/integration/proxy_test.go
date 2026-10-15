//go:build integration

package integration

import (
	"io"
	"net"
	"net/url"
	"sync"
	"testing"
)

// tcpProxy forwards TCP connections to a target and can simulate an outage:
// Cut() drops every open connection and refuses new ones until Restore().
type tcpProxy struct {
	ln      net.Listener
	target  string
	mu      sync.Mutex
	enabled bool
	conns   map[net.Conn]struct{}
}

func newProxy(t *testing.T, target string) *tcpProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &tcpProxy{ln: ln, target: target, enabled: true, conns: map[net.Conn]struct{}{}}
	go p.serve()
	t.Cleanup(func() { ln.Close(); p.Cut() })
	return p
}

func (p *tcpProxy) Addr() string { return p.ln.Addr().String() }

func (p *tcpProxy) serve() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		enabled := p.enabled
		p.mu.Unlock()
		if !enabled {
			c.Close()
			continue
		}
		u, err := net.Dial("tcp", p.target)
		if err != nil {
			c.Close()
			continue
		}
		p.mu.Lock()
		p.conns[c], p.conns[u] = struct{}{}, struct{}{}
		p.mu.Unlock()
		go p.pipe(c, u)
		go p.pipe(u, c)
	}
}

func (p *tcpProxy) pipe(dst, src net.Conn) {
	_, _ = io.Copy(dst, src)
	dst.Close()
	src.Close()
	p.mu.Lock()
	delete(p.conns, dst)
	delete(p.conns, src)
	p.mu.Unlock()
}

func (p *tcpProxy) Cut() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.enabled = false
	for c := range p.conns {
		c.Close()
	}
	p.conns = map[net.Conn]struct{}{}
}

func (p *tcpProxy) Restore() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.enabled = true
}

// viaProxy rewrites the host:port of a URL.
func viaProxy(raw, addr string) string {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	u.Host = addr
	return u.String()
}

func hostOf(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		panic(err)
	}
	return u.Host
}
