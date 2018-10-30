/*
  Copyright (C) 2018 Simon Schmidt
  
  This Source Code Form is subject to the terms of the Mozilla Public
  License, v. 2.0. If a copy of the MPL was not distributed with this
  file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

package core

import "github.com/byte-mug/walcast/nodes"

func CreateNodeEngine(name,group string,config IConfig) *NodeEngine {
	ne := new(NodeEngine)
	obs := new(nodes.ClusterObserver).Init()
	obs.Del = ne
	ne.Self.Name = name
	ne.Self.Group = group
	ne.Pool.Obs = obs
	ne.Pool.Init()
	ne.Config = config
	ne.Init()
	return ne
}