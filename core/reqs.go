/*
  Copyright (C) 2018 Simon Schmidt
  
  This Source Code Form is subject to the terms of the Mozilla Public
  License, v. 2.0. If a copy of the MPL was not distributed with this
  file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

package core

import "github.com/byte-mug/walcast/nodes"
//import "github.com/valyala/gorpc"
import "github.com/byte-mug/walcast/rpcdp"
import "github.com/valyala/gorpc"

const (
	S_NODEENGINE = "NodeEngine"
)

type requestNodeInfo struct{
	From *nodes.NodeRpc
}
/*
 Alone  = [_,$Self]
 Master = [$Self,_]
 Slave  = [$Master,$Self]
*/
type responseNodeInfo struct{
	Master,Slave *nodes.NodeRpc
}
type requestHookUp struct{
	From    *nodes.NodeRpc
}
type responseHookUp struct{
	Master *nodes.NodeRpc
}
type requestHookOff struct{
	From    *nodes.NodeRpc
}
type replicate struct{
	From    *nodes.NodeRpc
	Config interface{}
}
type redirect struct{
	Proposed *nodes.NodeRpc
}

func(ne *NodeEngine) hookUp(target string) {
	ne.stateLk.Lock(); defer ne.stateLk.Unlock()
	if ne.State()!=ALONE { return }
restart:
	resp,err := ne.Pool.Get(target).Call(rpcdp.Request{S_NODEENGINE,requestNodeInfo{&ne.Self}})
	if err!=nil { return }
	resp2,ok := resp.(responseNodeInfo)
	if !ok { return }
	if resp2.Master!=nil && resp2.Slave!=nil {
		target = resp2.Master.Name
		goto restart
	}
	resp,err = ne.Pool.Get(target).Call(rpcdp.Request{S_NODEENGINE,requestHookUp{&ne.Self}})
	if err!=nil { return }
	resp3,ok := resp.(responseHookUp)
	if !ok { return }
	if resp3.Master==nil { return }
	ne.master = resp3.Master
}
func(ne *NodeEngine) hookOff() {
	ne.stateLk.Lock(); defer ne.stateLk.Unlock()
	_,err := ne.Pool.Get(ne.master.Name).Call(rpcdp.Request{S_NODEENGINE,requestHookOff{&ne.Self}})
	if err!=nil { return }
	ne.master = nil
}
func(ne *NodeEngine) recheck() {
	ne.stateLk.RLock(); defer ne.stateLk.RUnlock()
	target,ok := ne.master.GetName()
	if !ok { return }
restart:
	resp,err := ne.Pool.Get(target).Call(rpcdp.Request{S_NODEENGINE,requestNodeInfo{&ne.Self}})
	if err!=nil { return }
	resp2,ok := resp.(responseNodeInfo)
	if !ok { return }
	if resp2.Master!=nil && resp2.Slave!=nil {
		target = resp2.Master.Name
		goto restart
	}
	go func() {
		ne.hookOff()
		ne.hookUp(target)
	}()
}

func(ne *NodeEngine) pushSlaves() {
	ne.stateLk.RLock(); defer ne.stateLk.RUnlock()
	if ne.dirty {
		i := rpcdp.Request{S_NODEENGINE,replicate{&ne.Self,ne.Config.Export()}}
		for n := ne.slaves.Left(); n!=nil ; n = n.Next() {
			ne.Pool.Get(n.Key.(string)).Send(i)
		}
		ne.dirty = false
	}
}
func(ne *NodeEngine) redirectSlaves(n *nodes.NodeRpc) {
	ne.stateLk.RLock(); defer ne.stateLk.RUnlock()
	i := rpcdp.Request{S_NODEENGINE,redirect{n}}
	for n := ne.slaves.Left(); n!=nil ; n = n.Next() {
		ne.Pool.Get(n.Key.(string)).Send(i)
	}
}


func(ne *NodeEngine) handle(clientAddr string, request interface{}) (response interface{}) {
	switch R := request.(type){
	case requestNodeInfo:
		ne.stateLk.RLock(); defer ne.stateLk.RUnlock()
		rni := responseNodeInfo{ne.master,&ne.Self}
		if rni.Master==nil && ne.slaves.Root!=nil {
			rni = responseNodeInfo{rni.Slave,nil}
		}
		return rni
	case requestHookUp:
		ne.stateLk.Lock(); defer ne.stateLk.Unlock()
		if ne.master!=nil { return responseHookUp{} }
		ne.slaves.Put(R.From.Name,R.From)
		ne.Config.SetNodeNameList(ne.slaves.Keys())
		ne.dirty = true
		return responseHookUp{&ne.Self}
	case requestHookOff:
		ne.stateLk.Lock(); defer ne.stateLk.Unlock()
		ne.slaves.Remove(R.From.Name)
		ne.Config.SetNodeNameList(ne.slaves.Keys())
		ne.dirty = true
		return "done!"
	case replicate:
		ne.stateLk.RLock(); defer ne.stateLk.RUnlock()
		if !ne.master.Match(R.From) { return "error!" }
		ne.Config.Import(R.Config)
		return "done!"
	case redirect:
		go func(){
			ne.hookOff()
			if R.Proposed!=nil {
				ne.hookUp(R.Proposed.Name)
			}
		}()
		return "done!"
	}
	return "error!"
}

func init(){
	gorpc.RegisterType(requestNodeInfo{})
	gorpc.RegisterType(responseNodeInfo{})
	
	gorpc.RegisterType(requestHookUp{})
	gorpc.RegisterType(responseHookUp{})
	
	gorpc.RegisterType(requestHookOff{})
	
}
