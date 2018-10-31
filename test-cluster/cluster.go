/*
  Copyright (C) 2018 Simon Schmidt
  
  This Source Code Form is subject to the terms of the Mozilla Public
  License, v. 2.0. If a copy of the MPL was not distributed with this
  file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

// Test-Cluster setup
package tc

import "github.com/hashicorp/memberlist"
import "github.com/byte-mug/walcast/hccm"
import "github.com/byte-mug/walcast/core"
import "github.com/byte-mug/walcast/rpcdp"
import "github.com/nu7hatch/gouuid"
import "github.com/valyala/gorpc"
import "fmt"
import "time"

var my_group string

func create(port int, cfg core.IConfig) (
	*memberlist.Config,
	*rpcdp.Dispatcher,
	*gorpc.Server,
	*core.NodeEngine ){
	u,err := uuid.NewV4()
	if err!=nil { panic(err) }
	ne := core.CreateNodeEngine(u.String(),my_group,cfg)
	mlc := memberlist.DefaultLocalConfig()
	mlc.Name = u.String()
	mlc.BindPort = port
	mlc.AdvertisePort = port
	disp,srv := hccm.CreateConfig(mlc,ne,port+1)
	return mlc,disp,srv,ne
}

type Function func() (core.IConfig,func(*rpcdp.Dispatcher,*core.NodeEngine))

func Startup(fu Function,num int) {
	prepare()
	port := 7946
	addr := make([]string,0,num)
	srvs := make([]*gorpc.Server,0,num)
	for i:=0 ; i<num ; i++ {
		cfg,cb := fu()
		mlc,disp,srv,ne := create(port,cfg)
		cb(disp,ne)
		addr = append(addr,fmt.Sprintf("127.0.0.1:%d",port))
		port += 2
		ml,err := memberlist.Create(mlc)
		if err!=nil { panic(err) }
		if len(addr)>0 {
			ml.Join(addr)
		}
		srv.Start()
		srvs = append(srvs,srv)
		time.Sleep(time.Second/10)
	}
}

func prepare() {
	u,err := uuid.NewV4()
	if err!=nil { u = new(uuid.UUID) }
	my_group = u.String()
}
