/*
  Copyright (C) 2018 Simon Schmidt
  
  This Source Code Form is subject to the terms of the Mozilla Public
  License, v. 2.0. If a copy of the MPL was not distributed with this
  file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

// Core
package core

import avl "github.com/emirpasic/gods/trees/avltree"
import "github.com/emirpasic/gods/utils"
import "time"
//import "github.com/valyala/gorpc"
import "github.com/byte-mug/walcast/nodes"
import "github.com/byte-mug/walcast/rpc"
//import "github.com/byte-mug/walcast/rpcdp"
import "sync"

type nodeEvent struct{
	enter bool
	name  string
}
type State uint
const (
	ALONE  State = iota
	MASTER
	SLAVE
)

type IConfig interface{
	SetNodeNameList(s []interface{})
	Export() interface{}
	Import(i interface{})
}

type NodeEngine struct {
	Self     nodes.NodeRpc
	Pool     rpc.ConnectionPool
	Config   IConfig
	dirty    bool
	master   *nodes.NodeRpc
	slaves   *avl.Tree
	nodeEvs  chan nodeEvent
	stateLk  sync.RWMutex
}
func(ne *NodeEngine) Init() {
	ne.Self.Startup = time.Now().UTC()
	ne.Pool.Init()
	ne.slaves  = avl.NewWith(utils.StringComparator)
	ne.nodeEvs = make(chan nodeEvent,16)
	go ne.perform()
}
func(ne *NodeEngine) State() State {
	if ne.master!=nil {
		return SLAVE
	} else if ne.slaves.Root!=nil {
		return MASTER
	}
	return ALONE
}

func(ne *NodeEngine) perform() {
	ticker := time.Tick(time.Second)
	refresh := time.Tick(time.Second)
	for {
		select {
		case ev := <- ne.nodeEvs:
			if ev.enter {
				ne.enter(ev.name)
			} else {
				ne.leave(ev.name)
			}
		case <- ticker:
			ne.pushSlaves()
		case <- refresh:
			switch ne.State() {
			case ALONE:
				// TODO hook me up.
			case SLAVE:
				ne.recheck()
			}
		}
	}
}
func(ne *NodeEngine) enter(n string) {
	node,ok := ne.Pool.Obs.Map.Get(n)
	if !ok { return }
	if node.Name==ne.Self.Name { return }
	switch ne.State() {
	case ALONE:
		if node.Startup.Before(ne.Self.Startup) {
			go ne.hookUp(node.Name)
		}
	case MASTER:
		if node.Startup.Before(ne.Self.Startup) {
			go func() {
				ne.hookUp(node.Name)
				time.Sleep(time.Second)
				ne.redirectSlaves(node.Rpc())
			}()
		}
	case SLAVE: // ignore
	}
}
func(ne *NodeEngine) leave(n string) {
	node,ok := ne.Pool.Obs.Map.Get(n)
	if !ok { return }
	if node.Name==ne.Self.Name { return }
	ne.stateLk.Lock(); defer ne.stateLk.Unlock()
	ne.slaves.Remove(node.Name)
	if ne.master==nil { return }
	if ne.master.Name == node.Name { ne.master=nil }
}

func(ne *NodeEngine) OnEnter(n string) {
	ne.nodeEvs <- nodeEvent{true,n}
}
func(ne *NodeEngine) OnLeave(n string) {
	ne.nodeEvs <- nodeEvent{false,n}
}

