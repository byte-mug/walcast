/*
  Copyright (C) 2018 Simon Schmidt
  
  This Source Code Form is subject to the terms of the Mozilla Public
  License, v. 2.0. If a copy of the MPL was not distributed with this
  file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

// Cluster (-Config) Management
package nodes

import avl "github.com/emirpasic/gods/trees/avltree"
import "github.com/emirpasic/gods/utils"
import "net"
import "io"
import "time"
import "sync"
import "fmt"

type Node struct{
	*Metadata
	Addr    net.IP
	Name    string
}
type NodeRpc struct{
	Name    string
	Startup time.Time
	Group   string
}
type Metadata struct{
	RpcPort int
	Startup time.Time
	Group   string
}

type Map struct{
	st,nt *avl.Tree
	sm,nm sync.RWMutex
}
func (m *Map) Init() *Map {
	m.st = avl.NewWith(utils.TimeComparator)
	m.nt = avl.NewWith(utils.StringComparator)
	return m
}
func (m *Map) Add(n *Node) {
	m.sm.Lock(); defer m.sm.Unlock()
	m.nm.Lock(); defer m.nm.Unlock()
	m.st.Put(n.Startup,n)
	m.nt.Put(n.Name,n)
}
func (m *Map) Remove(n *Node) {
	m.sm.Lock(); defer m.sm.Unlock()
	m.nm.Lock(); defer m.nm.Unlock()
	m.st.Remove(n.Startup)
	m.nt.Remove(n.Name)
}
func (m *Map) Get(name string) (*Node,bool) {
	m.nm.RLock(); defer m.nm.RUnlock()
	v,ok := m.nt.Get(name)
	if !ok { return nil,false }
	n,ok := v.(*Node)
	return n,ok
}
func (m *Map) AllOlderNodes(self string) []*Node {
	m.sm.RLock(); defer m.sm.RUnlock()
	r := make([]*Node,0,128)
	for n := m.st.Left(); n!=nil; n = n.Next() {
		nd,ok := n.Value.(*Node)
		if !ok { continue }
		if nd.Name==self { break }
		r = append(r,nd)
	}
	return r
}

type IDelegate interface {
	OnEnter(n string)
	OnLeave(n string)
}
type ClusterObserver struct {
	Group string
	Name  string
	Map   *Map
	Del   IDelegate
}
func (c *ClusterObserver) Dial(addr string) (conn io.ReadWriteCloser, err error) {
	n,ok := c.Map.Get(addr)
	if !ok { return nil,fmt.Errorf("Not found: %s",addr) }
	return net.DialTCP("tcp",nil,&net.TCPAddr{IP:n.Addr,Port:n.RpcPort})
}
func (c *ClusterObserver) Init() *ClusterObserver {
	c.Map = new(Map).Init()
	return c
}
func (c *ClusterObserver) OnEnter(n *Node) {
	c.Map.Add(n)
	if c.Name!=n.Name {
		c.Del.OnEnter(n.Name)
	}
}
func (c *ClusterObserver) OnLeave(name string) {
	n,ok := c.Map.Get(name)
	if ok { c.Map.Remove(n) }
	if c.Name!=name {
		c.Del.OnLeave(name)
	}
}


