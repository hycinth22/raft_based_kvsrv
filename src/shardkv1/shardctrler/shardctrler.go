package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"

	"log"
)

const CONFIG_KEY = "ShardCtrlerConfig"

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk
}

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// log.Println("[ShardCtrler] start up")
	return sck
}

// The tester calls InitController() before starting a new
// controller. In part A, this method doesn't need to do anything. In
// B and C, this method implements recovery.
func (sck *ShardCtrler) InitController() {
}

// Called once by the tester to supply the first configuration.  You
// can marshal ShardConfig into a string using shardcfg.String(), and
// then Put it in the kvsrv for the controller at version 0.  You can
// pick the key to name the configuration.  The initial configuration
// lists shardgrp shardcfg.Gid1 for all shards.
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	sck.putConfig(cfg, rpc.Tversion(0))
}

// Called by the tester to ask the controller to change the
// configuration from the current one to new.  While the controller
// changes the configuration it may be superseded by another
// controller.
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// migrate
	old, oldVersion := sck.getConfig()
	//log.Printf("[ChangeConfigTo] old %#v new %#v", old, new)
	for shardId := shardcfg.Tshid(0); shardId<shardcfg.NShards; shardId++ {
		oldgid, oldsrvs, ok := old.GidServers(shardId)
		if !ok {
			log.Println("[ChangeConfigTo] get old.GidServers failed.", "shardId", shardId)
			return
		}
		newgid, newsrvs, ok := new.GidServers(shardId)
		if !ok {
			log.Println("[ChangeConfigTo] get new.GidServers failed.", "shardId", shardId)
			return
		}
		if oldgid != newgid {
			oldclk, newclk := shardgrp.MakeClerk(sck.clnt, oldsrvs), shardgrp.MakeClerk(sck.clnt, newsrvs)
			shard, err := oldclk.FreezeShard(shardId, new.Num)
			if err != rpc.OK {
				log.Println("[ChangeConfigTo] FreezeShard error: ", err, "shardId", shardId, "oldgid", oldgid, "newgid", newgid)
				return
			}
			err = newclk.InstallShard(shardId, shard, new.Num)
			if err != rpc.OK {
				log.Println("[ChangeConfigTo] InstallShard error: ", err, "shardId", shardId, "oldgid", oldgid, "newgid", newgid)
				return
			}
			err = oldclk.DeleteShard(shardId, new.Num)
			if err != rpc.OK {
				log.Println("[ChangeConfigTo] DeleteShard error: ", err, "shardId", shardId, "oldgid", oldgid, "newgid", newgid)
				return
			}
		}
	}
	// save new config
	err := sck.putConfig(new, oldVersion)
	log.Println("[ChangeConfigTo] putConfig error:", err)
	//log.Printf("[ChangeConfigTo] new config applied. %#v", new)
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	config, _ := sck.getConfig()
	return config
}

func (sck *ShardCtrler) getConfig() (*shardcfg.ShardConfig, rpc.Tversion) {
	configdata, version, err := sck.IKVClerk.Get(CONFIG_KEY)
	if err != rpc.OK {
		log.Println("ShardCtrler getConfig RPC error:", err)
		return nil, rpc.Tversion(0)
	}
	config := shardcfg.FromString(configdata)
	return config, version
}

func (sck *ShardCtrler) putConfig(cfg *shardcfg.ShardConfig, oldVersion rpc.Tversion) rpc.Err {
	cfgdata := cfg.String()
	err := sck.IKVClerk.Put(CONFIG_KEY, cfgdata, oldVersion)
	if err != rpc.OK {
		log.Println("ShardCtrler putConfig RPC error:", err)
		return err
	}
	return rpc.OK
}
