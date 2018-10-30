/*
  Copyright (C) 2018 Simon Schmidt
  
  This Source Code Form is subject to the terms of the Mozilla Public
  License, v. 2.0. If a copy of the MPL was not distributed with this
  file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/


package hccm

import "github.com/byte-mug/walcast/core"
import "github.com/hashicorp/memberlist"
import "github.com/vmihailenco/msgpack"
import "github.com/byte-mug/walcast/rpcdp"
import "github.com/valyala/gorpc"
import "fmt"

type fixedDelegate struct{
	meta []byte
}
func (f *fixedDelegate) NodeMeta(limit int) []byte {
	if len(f.meta)>limit { return nil }
	return f.meta
}

func (f *fixedDelegate) NotifyMsg([]byte) {}
func (f *fixedDelegate)  GetBroadcasts(overhead, limit int) [][]byte { return nil }
func (f *fixedDelegate) LocalState(join bool) []byte { return nil }
func (f *fixedDelegate) MergeRemoteState(buf []byte, join bool) { }

func CreateConfig(conf *memberlist.Config,ne *core.NodeEngine, port int) (*rpcdp.Dispatcher,*gorpc.Server) {
	meta,_ := msgpack.Marshal(ne.Self.Metadata(port))
	conf.Name = ne.Self.Name
	conf.Delegate = &fixedDelegate{meta}
	del := &ObserverDelegate{ne.Pool.Obs}
	conf.Events = del
	conf.Merge = del
	conf.Alive = del
	disp := new(rpcdp.Dispatcher).Init()
	disp.Service.Put(core.S_NODEENGINE,gorpc.HandlerFunc(ne.Handle))
	
	return disp,gorpc.NewTCPServer(fmt.Sprintf(":%d",port),disp.Handler())
}

