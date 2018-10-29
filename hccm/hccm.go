/*
  Copyright (C) 2018 Simon Schmidt
  
  This Source Code Form is subject to the terms of the Mozilla Public
  License, v. 2.0. If a copy of the MPL was not distributed with this
  file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

// Cluster Membership based on memberlist.
package hccm

import "github.com/hashicorp/memberlist"
import "github.com/vmihailenco/msgpack"
import "github.com/byte-mug/walcast/nodes"
import "fmt"


type ObserverDelegate struct{
	Obs *nodes.ClusterObserver
}
func (o *ObserverDelegate) NotifyMerge(peers []*memberlist.Node) error {
	for _,peer := range peers {
		md := new(nodes.Metadata)
		err := msgpack.Unmarshal(peer.Meta,&md)
		if err!=nil { return err }
		if md.Group!=o.Obs.Group { return fmt.Errorf("Group mismatch %s!=%s",md.Group,o.Obs.Group) }
	}
	return nil
}
func (o *ObserverDelegate) NotifyAlive(peer *memberlist.Node) error {
	md := new(nodes.Metadata)
	err := msgpack.Unmarshal(peer.Meta,&md)
	if err!=nil { return err }
	if md.Group!=o.Obs.Group { return fmt.Errorf("Group mismatch %s!=%s",md.Group,o.Obs.Group) }
	return nil
}

// NotifyJoin is invoked when a node is detected to have joined.
// The Node argument must not be modified.
func (o *ObserverDelegate) NotifyJoin(n *memberlist.Node) {
	md := new(nodes.Metadata)
	err := msgpack.Unmarshal(n.Meta,&md)
	if err!=nil { return }
	if md.Group!=o.Obs.Group { return }
	o.Obs.OnEnter(&nodes.Node{md,n.Addr,n.Name})
}

// NotifyLeave is invoked when a node is detected to have left.
// The Node argument must not be modified.
func (o *ObserverDelegate) NotifyLeave(n *memberlist.Node) {
	o.Obs.OnLeave(n.Name)
}

// NotifyUpdate is invoked when a node is detected to have
// updated, usually involving the meta data. The Node argument
// must not be modified.
func (o *ObserverDelegate) NotifyUpdate(*memberlist.Node) {}

