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
	"fmt"
	"io"
)

const CONFIG_KEY = "ShardCtrlerConfig"
const CONFIG_MIGRATING_NEW_KEY = "ShardCtrlerMigratingNewConfig"

func init() {
	log.SetOutput(io.Discard)
}

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

// Calls InitController() before starting a new controller.
func (sck *ShardCtrler) InitController() {
	sck.checkAndCompleteMirgratingNewConfig()
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
	var mcfgver rpc.Tversion
	var err error
	for {
		mcfgver, err = sck.checkAndCompleteMirgratingNewConfig()
		if err != nil {
			panic(err)
		}
		log.Println("start ChangeConfigTo")
		rerr := sck.putMigratingNewConfig(new, mcfgver)
		if rerr == rpc.ErrVersion || rerr == rpc.ErrMaybe {
			continue // retry to check
		}
		if rerr != rpc.OK {
			panic(rerr)
		}
		mcfgver++
		break
	}
	sck.changeConfigTo(new, false)
	rerr := sck.deleteMigratingNewConfig(mcfgver)
	if rerr != rpc.OK {
		// ErrMaybe is ok, just make sure mcfgver is right
		if rerr == rpc.ErrMaybe {
			log.Println("[WARNING] deleteMigratingNewConfig result ErrMaybe")
		} else {
			panic(rerr)
		}
	}
	log.Println("complete ChangeConfigTo")
}

func (sck *ShardCtrler) checkAndCompleteMirgratingNewConfig() (rpc.Tversion, error) {
	// detect wheher exists an interrupted configuration mirgrating
	mcfg, mcfgver := sck.getMigratingNewConfig()
	if mcfg != nil {
		// recovery from an interrupted configuration mirgrating
		log.Println("detect an interrupted configuration mirgrating, recovering")
		sck.changeConfigTo(mcfg, true)
		err := sck.deleteMigratingNewConfig(mcfgver)
		if err != rpc.OK {
			// ErrMaybe is ok, just make sure mcfgver is right
			if err == rpc.ErrMaybe {
				log.Println("[WARNING] deleteMigratingNewConfig result ErrMaybe")
			} else {
				log.Println("recover from an interrupted configuration mirgrating, error:", err)
				return 0, fmt.Errorf("recover from an interrupted configuration mirgrating, error: %v", err)
			}
		}
		log.Println("recovered from an interrupted configuration mirgrating")
	}
	return mcfgver, nil
}


func (sck *ShardCtrler) changeConfigTo(new *shardcfg.ShardConfig, isrecover bool) {
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
			shard, err := freezeShard(oldclk, shardId, new.Num)
			if err != rpc.OK {
				if isrecover && err == rpc.ErrNoShard {
					log.Println("[ChangeConfigTo] FreezeShard report noshard. maybe moving this shard has been completed by previous shardctrler.... continue", "shardId", shardId, "oldgid", oldgid, "newgid", newgid)
					continue // process next shard
				} else {
					log.Println("[ChangeConfigTo] FreezeShard error: ", err, "shardId", shardId, "oldgid", oldgid, "newgid", newgid)
					return
				}
			}
			err = installShard(newclk, shardId, shard, new.Num)
			if err != rpc.OK {
				log.Println("[ChangeConfigTo] InstallShard error: ", err, "shardId", shardId, "oldgid", oldgid, "newgid", newgid)
				return
			}
			err = deleteShard(oldclk, shardId, new.Num)
			if err != rpc.OK {
				log.Println("[ChangeConfigTo] DeleteShard error: ", err, "shardId", shardId, "oldgid", oldgid, "newgid", newgid)
				return
			}
		}
	}
	// save new config
	err := sck.putConfig(new, oldVersion)
	if err != rpc.OK {
		log.Println("[ChangeConfigTo] putConfig error:", err)
		return
	}
	//log.Printf("[ChangeConfigTo] new config applied. %#v", new)
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	config, _ := sck.getConfig()
	return config
}

func (sck *ShardCtrler) getConfig() (*shardcfg.ShardConfig, rpc.Tversion) {
	configdata, version, err := sck.IKVClerk.Get(CONFIG_KEY)
	if err != rpc.OK && err != rpc.ErrNoKey {
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

// return rpc.OK if ok
func (sck *ShardCtrler) getMigratingNewConfig() (*shardcfg.ShardConfig, rpc.Tversion) {
	configdata, version, err := sck.IKVClerk.Get(CONFIG_MIGRATING_NEW_KEY)
	for err != rpc.OK && err != rpc.ErrNoKey {
		log.Println("ShardCtrler getMigratingNewConfig RPC error:", err)
		configdata, version, err = sck.IKVClerk.Get(CONFIG_MIGRATING_NEW_KEY)
	}
	if configdata == "" {
		return nil, version
	}
	config := shardcfg.FromString(configdata)
	return config, version
}

// return rpc.OK if ok
// return rpc.ErrVersion if oldVersion donot match
// return rpc.ErrMaybe if ErrVersion but in retring....
func (sck *ShardCtrler) putMigratingNewConfig(cfg *shardcfg.ShardConfig, oldVersion rpc.Tversion) rpc.Err {
	cfgdata := cfg.String()
	err := sck.IKVClerk.Put(CONFIG_MIGRATING_NEW_KEY, cfgdata, oldVersion)
	if err != rpc.OK {
		log.Println("ShardCtrler putMigratingNewConfig RPC error:", err)
		return err
	}
	return rpc.OK
}

// return rpc.OK if ok
// return rpc.ErrVersion if oldVersion donot match
// return rpc.ErrMaybe if ErrVersion but in retring....
func (sck *ShardCtrler) deleteMigratingNewConfig(oldVersion rpc.Tversion) rpc.Err {
	err := sck.IKVClerk.Put(CONFIG_MIGRATING_NEW_KEY, "", oldVersion)
	if err != rpc.OK {
		log.Println("ShardCtrler putMigratingNewConfig RPC error:", err)
		return err
	}
	return rpc.OK
}

// freezeShard
// retry while ErrgGroupMaybeLeave
func freezeShard(sgck *shardgrp.Clerk, s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	shard, err := sgck.FreezeShard(s, num)
	for err == rpc.ErrgGroupMaybeLeave {
		shard, err = sgck.FreezeShard(s, num)
	}
	return shard, err
}

// installShard
// retry while ErrgGroupMaybeLeave
func installShard(sgck *shardgrp.Clerk, s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	err := sgck.InstallShard(s, state, num)
	for err == rpc.ErrgGroupMaybeLeave {
		err = sgck.InstallShard(s, state, num)
	}
	return err
}

// deleteShard
// retry while ErrgGroupMaybeLeave
func deleteShard(sgck *shardgrp.Clerk, s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	err := sgck.DeleteShard(s, num)
	for err == rpc.ErrgGroupMaybeLeave {
		err = sgck.DeleteShard(s, num)
	}
	return err
}