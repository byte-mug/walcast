/*
  Copyright (C) 2018 Simon Schmidt
  
  This Source Code Form is subject to the terms of the Mozilla Public
  License, v. 2.0. If a copy of the MPL was not distributed with this
  file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

// RPC-Base
package core

//import "time"
//import "github.com/valyala/gorpc"
import "github.com/byte-mug/walcast/nodes"
//import "github.com/byte-mug/walcast/rpc"

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

type NodeEngine struct {
	nodes   *nodes.Map
	state   State
	nodeEvs chan nodeEvent
}
func(ne *NodeEngine) Init() {
	ne.nodes   = new(nodes.Map).Init()
	ne.state   = ALONE
	ne.nodeEvs = make(chan nodeEvent,16)
	go ne.perform()
}
func(ne *NodeEngine) perform() {
	for {
		select {
		case ev := <- ne.nodeEvs:
			if ev.enter {
				ne.enter(ev.name)
			} else {
				ne.leave(ev.name)
			}
		}
	}
}
func(ne *NodeEngine) enter(n string) {
	
	switch ne.state {
	case ALONE:
		
	}
}
func(ne *NodeEngine) leave(n string) {
	
}

func(ne *NodeEngine) OnEnter(n string) {
	ne.nodeEvs <- nodeEvent{true,n}
}
func(ne *NodeEngine) OnLeave(n string) {
	ne.nodeEvs <- nodeEvent{false,n}
}

