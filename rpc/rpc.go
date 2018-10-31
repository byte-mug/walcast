/*
  Copyright (C) 2018 Simon Schmidt
  
  This Source Code Form is subject to the terms of the Mozilla Public
  License, v. 2.0. If a copy of the MPL was not distributed with this
  file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

// RPC-Base
package rpc

import "github.com/valyala/gorpc"
import "github.com/byte-mug/walcast/nodes"
import "sync"

type ConnectionPool struct {
	Obs   *nodes.ClusterObserver
	conns map[string]*gorpc.Client
	lock  sync.RWMutex
}
func (cp *ConnectionPool) Init() *ConnectionPool {
	cp.conns = make(map[string]*gorpc.Client)
	return cp
}
func (cp *ConnectionPool) Create(name string) *gorpc.Client {
	c := &gorpc.Client {
		Addr: name,
		Dial: cp.Obs.Dial,
	}
	c.Start()
	return c
}
func (cp *ConnectionPool) peek(name string) (*gorpc.Client,bool) {
	cp.lock.RLock(); defer cp.lock.RUnlock()
	c,ok := cp.conns[name]
	return c,ok
}
func (cp *ConnectionPool) Get(name string) *gorpc.Client {
	if c,ok := cp.peek(name); ok { return c }
	cp.lock.Lock(); defer cp.lock.Unlock()
	c := cp.Create(name)
	cp.conns[name] = c
	return c
}

//
