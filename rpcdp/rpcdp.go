/*
  Copyright (C) 2018 Simon Schmidt
  
  This Source Code Form is subject to the terms of the Mozilla Public
  License, v. 2.0. If a copy of the MPL was not distributed with this
  file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

// RPC-Dispatch
package rpcdp

import "github.com/valyala/gorpc"
import avl "github.com/emirpasic/gods/trees/avltree"

type Request struct {
	Service string
	Inner interface{}
}

type Dispatcher struct{
	Default gorpc.HandlerFunc
	Service *avl.Tree
}
func (d *Dispatcher) Init() *Dispatcher {
	d.Service = avl.NewWithStringComparator()
	return d
}
func (d *Dispatcher) Handler() gorpc.HandlerFunc {
	return func(clientAddr string, request interface{}) (response interface{}) {
		switch v := request.(type) {
		case Request:
			if f,ok := d.Service.Get(v.Service); ok { return f.(gorpc.HandlerFunc)(clientAddr,v.Inner) }
		//case *Request:
		//	if f,ok := d.Service.Get(v.Service); ok { return f.(gorpc.HandlerFunc)(clientAddr,v.Inner) }
		}
		return d.Default(clientAddr,request)
	}
}

func init() {
	gorpc.RegisterType(Request{})
}

