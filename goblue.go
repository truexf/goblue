//todo: more funtions
//平滑重启流程：启动后向某位置写入pid,重启时先重命名原执行文件名，复制新执行文件，启动新的执行文件，开始侦听，向原来的进程发送信号通知关闭，原进程收到信号后关闭侦听，结束进程
//想了想,决定此功能不放在这一层实现，更合理的放在封装goblue的完整进程层面去实现。

//!!! goblue只foucs在最核心的tcp层反向代理功能，通ProxyEvent接口向外层提供扩展，诸如平滑重启、http层的反向代理（负载均衡）,ip安全策略...
//!!! 以及更加完善的状态信息接口都通过ProxyEvent接口来暴露给外层实现.
package goblue

import (
	"fmt"
	"github.com/golang/glog"
	"github.com/truexf/gocfg"
	"github.com/truexf/goutil"
	"io"
	"io/ioutil"
	"net"
	"strings"
	"sync"
	"time"
)

type ProxyEvent interface {
	//clientAddr remoteIp:port/localIp:port
	OnClientConnected(clientAddr string) bool //return true if connection is allowed else false
	//clientAddr remoteIp:port/localIp:port
	OnClientDisconnected(clientAddr string) //occured when client was disconnected
	//clientAddr remoteIp:port/localIp:port  backendAddr remoteIp:port/localIp:port
	OnBackendConnected(clientAddr, backendAddr string)
	//backendAddr remoteIp:port/localIp:port
	OnBackendDisconnected(backendAddr string)

	//data transfer event,you can change the data and return new data
	//backendAddr remoteIp:port/localIp:port
	OnClientRecved(clientAddr string, data []byte) []byte
	//clientAddr remoteIp:port/localIp:port
	OnClientSent(clientAddr string, bytes int)
	//backendAddr remoteIp:port/localIp:port
	OnBackendRecved(backendAddr string, data []byte) []byte
	//backendAddr remoteIp:port/localIp:port
	OnBackendSent(backendAddr string, bytes int)
}

type line struct {
	startTime   time.Time
	backendConn *net.TCPConn
	clientConn  *net.TCPConn
}

func NewLine(client, backend *net.TCPConn) *line {
	if client == nil || backend == nil {
		return nil
	}
	return &line{startTime: time.Now(), backendConn: backend, clientConn: client}
}

type ipMap struct {
	sync.Mutex
	data map[string]string //clientip:backendip:port
}

func NewIpMap(fn string) *ipMap {
	ret := new(ipMap)
	ret.data = make(map[string]string)
	if data, err := ioutil.ReadFile(fn); err == nil {
		dataSlice := strings.Split(string(data), "\n")
		for _, v := range dataSlice {
			l, r := goutil.SplitLR(v, ":")
			if l != "" && r != "" {
				ret.data[l] = r
			}
		}
	} else {
		glog.Errorf("load ipMap file %s fail.\n", fn)
	}
	return ret
}

func (m *ipMap) getBackend(clientIp string) string {
	m.Lock()
	defer m.Unlock()
	if ret, ok := m.data[clientIp]; ok {
		return ret
	}
	return ""
}

func (m *ipMap) deleteMap(clientIp string) bool {
	m.Lock()
	defer m.Unlock()
	if _, ok := m.data[clientIp]; ok {
		delete(m.data, clientIp)
		return true
	} else {
		return false
	}
}

func (m *ipMap) insertMap(clientIp, backendAddr string) {
	m.Lock()
	defer m.Unlock()
	m.data[clientIp] = backendAddr
}

type Proxy struct {
	listener      *net.TCPListener
	isIphash      bool
	ipHash        *ipMap
	cfg           *gocfg.GoConfig
	lines         map[string]*line //client(remoteip:port/localip:port):backend
	linesReverse  map[string]*line //backend(remoteip:port/localip:port):client
	linesLock     sync.Mutex
	backends      []string //ip:port
	backendsLock  sync.Mutex
	roundRobin    int
	eventInstance ProxyEvent
}

func NewProxy(cfgFile string, eventInstance ProxyEvent) (ret *Proxy, err error) {
	cfg, e := gocfg.NewGoConfig(cfgFile)
	if e != nil {
		return nil, fmt.Errorf("new proxy fail, %s", e.Error())
	}
	lsnAddr := cfg.Get("socket", "listen", "")
	if lsnAddr == "" {
		return nil, fmt.Errorf("get listen addr fail.")
	}
	backends := cfg.Get("proxy", "backends", "")
	if backends == "" {
		return nil, fmt.Errorf("get backends fail.")
	}
	ret = new(Proxy)
	ret.eventInstance = eventInstance
	ret.cfg = cfg
	ret.backends = strings.Split(backends, ",")
	if len(ret.backends) == 0 {
		return nil, fmt.Errorf("this is no backends.")
	}
	ret.roundRobin = -1
	ret.isIphash = cfg.GetBoolDefault("proxy", "iphash", false)
	ipHashFile := cfg.Get("proxy", "iphash-file", "/tmp/goblue-iphash")
	ret.ipHash = NewIpMap(ipHashFile)
	ret.lines = make(map[string]*line)
	ret.linesReverse = make(map[string]*line)
	return ret, nil
}

func (m *Proxy) setSocketOpt(conn *net.TCPConn) {
	if m.cfg == nil {
		return
	}

	if linger := m.cfg.GetIntDefault("socket", "linger", -1); linger > -1 {
		if e := conn.SetLinger(linger); e != nil {
			glog.Errorf("set linger fail,%s\n", e.Error())
		}
	}
	if keepAlive := m.cfg.GetIntDefault("socket", "keepalive", -1); keepAlive > -1 {
		if e := conn.SetKeepAlive(true); e != nil {
			glog.Errorf("set keepalive fail,%s\n", e.Error())
		} else {
			if e := conn.SetKeepAlivePeriod(time.Duration(keepAlive) * time.Second); e != nil {
				glog.Errorf("set keepalive fail,%s\n", e.Error())
			}
		}
	}
	if recv_buf_size := m.cfg.GetIntDefault("socket", "client-recv-buf-size", -1); recv_buf_size > 1024 {
		if e := conn.SetReadBuffer(recv_buf_size); e != nil {
			glog.Errorf("set read buf size fail,%s\n", e.Error())
		}
	}
	if send_buf_size := m.cfg.GetIntDefault("socket", "client-send-buf-size", -1); send_buf_size > 1024 {
		if e := conn.SetReadBuffer(send_buf_size); e != nil {
			glog.Errorf("set send buf size fail,%s\n", e.Error())
		}
	}
}

func (m *Proxy) nextBackend(clientIp string) string {
	m.backendsLock.Lock()
	defer m.backendsLock.Unlock()
	if len(m.backends) == 0 {
		glog.Errorf("this is no backends.\n")
		return ""
	}

	if m.isIphash {
		ret := m.ipHash.getBackend(clientIp)
		if ret != "" {
			for _, v := range m.backends {
				if v == ret {
					glog.V(3).Infof("ip hash, clientip: %s, backend: %s\n", clientIp, ret)
					return ret
				}
			}
		}
	}

	m.roundRobin++
	if m.roundRobin >= len(m.backends) {
		m.roundRobin = 0
	}
	glog.V(3).Infof("round robin %d, client %s, backend %s\n", clientIp, m.backends[m.roundRobin])
	return m.backends[m.roundRobin]
}

func (m *Proxy) deleteBackend(backendAddr string) {
	m.backendsLock.Lock()
	defer m.backendsLock.Unlock()
	idx := -1
	for i, v := range m.backends {
		if v == backendAddr {
			idx = i
			break
		}
	}
	if idx > -1 {
		glog.V(5).Infof("delete backend %s \n", backendAddr)
		for i := idx; i < len(m.backends)-1; i++ {
			m.backends[i] = m.backends[i+1]
		}
		m.backends = m.backends[:len(m.backends)-1]
	}
}

func (m *Proxy) addLine(client *net.TCPConn, backend *net.TCPConn) {
	if client == nil || backend == nil {
		return
	}
	m.linesLock.Lock()
	defer m.linesLock.Unlock()
	linekey := client.RemoteAddr().String() + "/" + client.LocalAddr().String()
	_, ok := m.lines[linekey]
	if !ok {
		ret := NewLine(client, backend)
		if ret != nil {
			m.lines[linekey] = ret
			linekeyReverse := backend.RemoteAddr().String() + "/" + backend.LocalAddr().String()
			m.linesReverse[linekeyReverse] = ret
			glog.V(3).Infof("add line, %s -> %s \n", linekey, linekeyReverse)
		}
	}

}

func (m *Proxy) deleteLineByClient(client *net.TCPConn) {
	m.linesLock.Lock()
	defer m.linesLock.Unlock()
	linekey := client.RemoteAddr().String() + "/" + client.LocalAddr().String()
	ln, ok := m.lines[linekey]
	if ok {
		linekeyReverse := ln.backendConn.RemoteAddr().String() + "/" + ln.backendConn.LocalAddr().String()
		glog.V(3).Infof("delete line, %s -> %s \n", linekey, linekeyReverse)

		if m.eventInstance != nil {
			go func() {
				m.eventInstance.OnClientDisconnected(client.RemoteAddr().String() + "/" + client.LocalAddr().String())
				m.eventInstance.OnBackendDisconnected(ln.backendConn.RemoteAddr().String() + "/" + ln.backendConn.LocalAddr().String())
			}()
		}

		ln.clientConn.Close()
		ln.backendConn.Close()
		delete(m.lines, linekey)
		delete(m.linesReverse, linekeyReverse)
	}
}

func (m *Proxy) deleteLineByBackend(backend *net.TCPConn) {
	m.linesLock.Lock()
	defer m.linesLock.Unlock()
	linekeyReverse := backend.RemoteAddr().String() + "/" + backend.LocalAddr().String()
	ln, ok := m.linesReverse[linekeyReverse]
	if ok {
		linekey := ln.clientConn.RemoteAddr().String() + "/" + ln.clientConn.LocalAddr().String()
		glog.V(3).Infof("delete line, %s -> %s \n", linekey, linekeyReverse)

		if m.eventInstance != nil {
			go func() {
				m.eventInstance.OnClientDisconnected(backend.RemoteAddr().String() + "/" + backend.LocalAddr().String())
				m.eventInstance.OnBackendDisconnected(ln.clientConn.RemoteAddr().String() + "/" + ln.clientConn.LocalAddr().String())
			}()
		}

		ln.clientConn.Close()
		ln.backendConn.Close()
		delete(m.lines, linekey)
		delete(m.linesReverse, linekeyReverse)
	}
}

func (m *Proxy) workFor(client, backend *net.TCPConn) {
	go m.workFor1(client, backend)
	buf := make([]byte, 81920)
	for {
		n, eRead := client.Read(buf)
		if n > 0 {
			if m.eventInstance != nil {
				buf = m.eventInstance.OnClientRecved(fmt.Sprintf("%s/%s", client.RemoteAddr().String(), client.LocalAddr().String()),
					buf)
			}
			if buf == nil {
				continue
			}

			bytes, eWrite := backend.Write(buf[:n])
			if eWrite != nil {
				glog.V(3).Infof("write to backend %s fail,%s\n", backend.RemoteAddr().String())
				m.deleteLineByBackend(backend)
				return
			} else {
				if m.eventInstance != nil {
					go func() {
						m.eventInstance.OnBackendSent(fmt.Sprintf("%s/%s", backend.RemoteAddr().String(), backend.LocalAddr().String()), bytes)
					}()
				}
			}
		}
		if eRead != nil {
			if eRead != io.EOF {
				glog.V(3).Infof("client %s disconnect unexcepted,%s\n", client.RemoteAddr().String(), eRead.Error())
			} else {
				glog.V(3).Infof("client %s disconnected, io eof.\n", client.RemoteAddr().String())
			}
			m.deleteLineByClient(client)
			return
		}
	}
}

func (m *Proxy) workFor1(client, backend *net.TCPConn) {
	buf := make([]byte, 81920)
	for {
		n, eRead := backend.Read(buf)
		if n > 0 {
			if m.eventInstance != nil {
				buf = m.eventInstance.OnBackendRecved(fmt.Sprintf("%s/%s", backend.RemoteAddr().String(), backend.LocalAddr().String()),
					buf)
			}
			if buf == nil {
				continue
			}

			bytes, eWrite := client.Write(buf[:n])
			if eWrite != nil {
				glog.V(3).Infof("write to client %s fail,%s\n", client.RemoteAddr().String())
				m.deleteLineByClient(client)
				return
			} else {
				if m.eventInstance != nil {
					go func() {
						m.eventInstance.OnClientSent(fmt.Sprintf("%s/%s", client.RemoteAddr().String(), client.LocalAddr().String()), bytes)
					}()
				}
			}
		}
		if eRead != nil {
			if eRead != io.EOF {
				glog.V(3).Infof("backend %s disconnect unexcepted,%s\n", backend.RemoteAddr().String(), eRead.Error())
			} else {
				glog.V(3).Infof("backend %s disconnected, io eof.\n", backend.RemoteAddr().String())
			}
			m.deleteLineByBackend(backend)
			return
		}
	}
}

func (m *Proxy) connectBackend(client *net.TCPConn, backendAddr string) bool {
	tcpAddr, addrErr := net.ResolveTCPAddr("tcp4", backendAddr)
	if addrErr != nil {
		glog.Errorf("invalid backend addr: %s\n", backendAddr)
		return false
	}
	conn, err := net.DialTCP("tcp4", nil, tcpAddr)
	if err == nil {
		m.setSocketOpt(conn)
		glog.V(3).Infof("connect backend %s success\n", backendAddr)
		m.addLine(client, conn)
		go m.workFor(client, conn)

		if m.eventInstance != nil {
			m.eventInstance.OnBackendConnected(fmt.Sprintf("%s/%s", client.RemoteAddr().String(), client.LocalAddr().String()),
				fmt.Sprintf("%s/%s", conn.RemoteAddr().String(), conn.LocalAddr().String()))
		}
	} else {
		glog.Errorf("connect backend fail, client %s  backend %s, %s\n", client.RemoteAddr().String(), backendAddr, err.Error())
	}
	return true
}

func (m *Proxy) clean() {

}

func (m *Proxy) StopListen() {
	glog.Infof("stoplisten.")
	if m.listener != nil {
		m.listener.Close()
	}
}

func (m *Proxy) StartListen() {
	addr := m.cfg.Get("socket", "listen", "")
	if addr == "" {
		glog.Errorf("listen addr undefined.\n")
		return
	}
	tcpAddr, err := net.ResolveTCPAddr("tcp4", addr)
	if err != nil {
		glog.Errorf("invalid listen addr %s\n", addr)
		return
	}
	lsn, errLsn := net.ListenTCP("tcp4", tcpAddr)
	if errLsn != nil {
		glog.Errorf("listen tcp on %s fail,%s\n", tcpAddr.String(), errLsn.Error())
		return
	}
	m.listener = lsn
	for {
		conn, errAccept := lsn.AcceptTCP()
		if errAccept != nil {
			glog.Errorf("accept slave connection fail,%s\n", errAccept)
			break
		}

		if m.eventInstance != nil {
			if !m.eventInstance.OnClientConnected(fmt.Sprintf("%s/%s", conn.RemoteAddr().String(), conn.LocalAddr().String())) {
				conn.Close()
				continue
			}
		}

		//set socketoption
		m.setSocketOpt(conn)

		connAddr := conn.RemoteAddr().String()
		l, _ := goutil.SplitLR(connAddr, ":")
		glog.V(3).Infof("accept slave conn %s\n", connAddr)
		backendAddr := m.nextBackend(l)
		if backendAddr == "" {
			glog.Errorf("get backend fail, connect will be closed.\n")
			conn.Close()
		} else {
			if !m.connectBackend(conn, backendAddr) {
				glog.Errorf("connect backend %s fail,connect will be closed.\n", backendAddr)
				conn.Close()
			}
		}
	}

	m.clean()
}
