//test code for goblue
package main

import (
	"fmt"
	"github.com/truexf/goblue"
	"io/ioutil"
)

type EventImplement struct {
}

//clientAddr remoteIp:port/localIp:port
func (e *EventImplement) OnClientConnected(clientAddr string) bool { //return true if connection is allowed else false
	return true
}

//clientAddr remoteIp:port/localIp:port
func (e *EventImplement) OnClientDisconnected(clientAddr string) { //occured when client was disconnected
	fmt.Printf("client disconnected,%s\n", clientAddr)
}

//clientAddr remoteIp:port/localIp:port  backendAddr remoteIp:port/localIp:port
func (e *EventImplement) OnBackendConnected(clientAddr, backendAddr string) {
	fmt.Printf("backend connected, %s\n", backendAddr)
}

//backendAddr remoteIp:port/localIp:port
func (e *EventImplement) OnBackendDisconnected(backendAddr string) {
	fmt.Printf("backend disconnected, %s\n", backendAddr)
}

//data transfer event,you can change the data and return new data
//backendAddr remoteIp:port/localIp:port
func (e *EventImplement) OnClientRecved(clientAddr string, data []byte) []byte {
	fmt.Printf("client %s ,recved: %s\n", clientAddr, string(data))
	return data
}

//clientAddr remoteIp:port/localIp:port
func (e *EventImplement) OnClientSent(clientAddr string, bytes int) {
	//
}

//backendAddr remoteIp:port/localIp:port
func (e *EventImplement) OnBackendRecved(backendAddr string, data []byte) []byte {
	fmt.Printf("backend %s ,recved: %s\n", backendAddr, string(data))
	return data
}

//backendAddr remoteIp:port/localIp:port
func (e *EventImplement) OnBackendSent(backendAddr string, bytes int) {
	//
}

func main() {
	cfg := `[socket]
listen=:3000
linger=0
keepalive=30,10
backend-recv-buf-size=87380
backend-send-buf-size=16384
client-recv-buf-size=87380
client-send-buf-size=16384

[proxy]
iphash=1
iphash-file=/tmp/gobulue-iphash
backends=127.0.0.1:8888`
	fn := "/tmp/goblue.cfg"
	ioutil.WriteFile(fn, []byte(cfg), 0666)
	if proxy, err := goblue.NewProxy(fn, new(EventImplement)); err != nil {
		fmt.Printf("new proxy fail, %s\n", err.Error())
		return
	} else {
		fmt.Println("listening...")
		proxy.StartListen()
	}
}
