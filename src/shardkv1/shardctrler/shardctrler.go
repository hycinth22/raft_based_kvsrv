package shardctrler

//
// Shardctrler with InitConfig, Query, and ChangeConfigTo methods
//

import (
	"6.5840/kvsrv1"
	"6.5840/kvsrv1/lock"
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/tester1"

	"fmt"
	"sync/atomic"
	"time"
)

const (
	CONFIG_KEY               = "ShardCtrlerConfig"
	CONFIG_MIGRATING_NEW_KEY = "ShardCtrlerMigratingNewConfig"

	LEASE_KEY     = "ShardCtrlerLease"
	LEASE_TIMEOUT = 1000 * time.Millisecond
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk
	me int64

	leaseLock *lock.Lock
	ownLease  bool
}

var sckCnt atomic.Int64

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	sck.me = sckCnt.Add(1)
	sck.leaseLock = lock.MakeExpiredLock(sck.IKVClerk, LEASE_KEY, LEASE_TIMEOUT)
	sck.dlogln("[ShardCtrler] start up", sck.me)
	return sck
}

func (sck *ShardCtrler) Id() int64 {
	return sck.me
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
	sck.ownLease = sck.leaseLock.TryAcquire()
	if !sck.ownLease {
		sck.dlogln("!sck.ownLease, ChangeConfigTo do no-op")
		return
	}
	sck.dlogln("sck.ownLease")
	var done atomic.Bool
	go func() {
		for !done.Load() {
			time.Sleep(LEASE_TIMEOUT / 10)
			ok := sck.leaseLock.ExtendExpiration()
			if !ok {
				sck.dlogln("sck.leaseLock.ExtendExpiration() fail")
			}
		}
	}()
	defer func() {
		done.Store(true)
	}()
	sck.dlogln("start ChangeConfigTo ", new)
	var mcfg *shardcfg.ShardConfig
	var mcfgver rpc.Tversion
	for {
		mcfg, mcfgver = sck.getMigratingNewConfig()
		sck.dlogln("mcfg", mcfg, "mcfgver", mcfgver)
		sck.dlogln("new", new)
		if mcfg != nil {
			sck.dlogln("different new config already deployed by other ctrler, new num", mcfg.Num, " deploying", new.Num)
			return // different new config already deployed by other ctrler
		}
		rerr := sck.putMigratingNewConfig(new, mcfgver)
		if rerr == rpc.ErrVersion {
			sck.dlogln("putMigratingNewConfig ErrVersion")
			continue // concurrent ctrler exists although leased, retry
		} else if rerr == rpc.ErrMaybe {
			sck.dlogln("putMigratingNewConfig ErrMaybe")
			break // regraded as successful update?
		} else if rerr != rpc.OK {
			sck.dlogln("putMigratingNewConfig err: ", rerr)
			panic(rerr)
		}
		sck.dlogln("putMigratingNewConfig over")
		break
	}
	mcfgver++
	sck.dlogln("putMigratingNewConfig ok", new)

	sck.changeConfigTo(new, false)

	rerr := sck.deleteMigratingNewConfig(mcfgver)
	if rerr != rpc.OK {
		// ErrMaybe is ok, just make sure mcfgver is right
		if rerr == rpc.ErrVersion || rerr == rpc.ErrMaybe {
			sck.dlogln("[WARNING] deleteMigratingNewConfig result ErrVersion or ErrMaybe.  Maybe a concurrent ctrler is running... or just network failure")
		} else {
			panic(rerr)
		}
	}
	sck.dlogln("complete ChangeConfigTo")
}

func (sck *ShardCtrler) checkAndCompleteMirgratingNewConfig() (rpc.Tversion, error) {
	// detect wheher exists an interrupted configuration mirgrating
	mcfg, mcfgver := sck.getMigratingNewConfig()
	if mcfg != nil {
		sck.dlogln("detect valid migratingNewConfig. wait for lease")
		sck.leaseLock.Acquire()
		sck.dlogln("checkAndCompleteMirgratingNewConfig sck.ownLease")
		var done atomic.Bool
		go func() {
			for !done.Load() {
				time.Sleep(LEASE_TIMEOUT / 10)
				sck.leaseLock.ExtendExpiration()
			}
		}()
		defer func() {
			done.Store(true)
		}()


		mcfg, mcfgver := sck.getMigratingNewConfig()
		if mcfg != nil {
			// recovery from an interrupted configuration mirgrating
			sck.dlogln("detect an interrupted configuration mirgrating, recovering", mcfg)
			sck.changeConfigTo(mcfg, true)
			err := sck.deleteMigratingNewConfig(mcfgver)
			if err != rpc.OK {
				// ErrMaybe is ok, just make sure mcfgver is right
				if err == rpc.ErrVersion || err == rpc.ErrMaybe {
					sck.dlogln("[WARNING] deleteMigratingNewConfig result ErrMaybe. Maybe a concurrent ctrler is running... or just network failure")
				} else {
					sck.dlogln("recover from an interrupted configuration mirgrating, error:", err)
					return 0, fmt.Errorf("recover from an interrupted configuration mirgrating, error: %v", err)
				}
			}
			sck.dlogln("recovered from an interrupted configuration mirgrating")
		}
	}
	return mcfgver, nil
}

func (sck *ShardCtrler) changeConfigTo(new *shardcfg.ShardConfig, isrecover bool) {
	// migrate
	old, oldVersion := sck.getConfig()
	if new.Num <= old.Num {
		sck.dlogln("[changeConfigTo] less or equal num detected. giveup", new.Num, "<=", old.Num)
		return
	}
	sck.dlog("[changeConfigTo] old %#v new %#v", old, new)
	for shardId := shardcfg.Tshid(0); shardId < shardcfg.NShards; shardId++ {
		oldgid, oldsrvs, ok := old.GidServers(shardId)
		if !ok {
			sck.dlogln("[changeConfigTo] get old.GidServers failed.", "shardId", shardId)
			return
		}
		newgid, newsrvs, ok := new.GidServers(shardId)
		if !ok {
			sck.dlogln("[changeConfigTo] get new.GidServers failed.", "shardId", shardId)
			return
		}
		if oldgid != newgid {
			sck.dlogln("moving shard", shardId , "from", oldgid, "to", newgid)
			oldclk, newclk := shardgrp.MakeClerk(sck.clnt, oldsrvs), shardgrp.MakeClerk(sck.clnt, newsrvs)
			sck.dlogln("freezeShard", shardId, "oldgid", oldgid, "new.Num", new.Num)
			shard, err := freezeShard(oldclk, shardId, new.Num)
			for err == rpc.ErrgGroupMaybeLeave {
				sck.dlogln("freezeShard err shardId", shardId, "oldgid", oldgid, "new.Num", new.Num, err)
				mcfg, _ := sck.getMigratingNewConfig()
				if mcfg == nil || mcfg.Num > new.Num {
					return
				}
				shard, err = freezeShard(oldclk, shardId, new.Num)
			}
			sck.dlogln("freezeShard over")
			if err != rpc.OK {
				if isrecover && err == rpc.ErrNoShard {
					sck.dlogln("[changeConfigTo] FreezeShard report noshard. maybe moving this shard has been completed by previous shardctrler.... continue. ", "shardId", shardId, "oldgid", oldgid, "newgid", newgid)
					continue // process next shard
				} else {
					sck.dlogln("[changeConfigTo] FreezeShard error: ", err, "shardId", shardId, "oldgid", oldgid, "newgid", newgid)
					return
				}
			}
			sck.dlogln("installShard", shardId, "newgid", newgid, "new.Num", new.Num)
			err = installShard(newclk, shardId, shard, new.Num)
			for err == rpc.ErrgGroupMaybeLeave {
				sck.dlogln("installShard err shardId", shardId, "newgid", newgid, "new.Num", new.Num, err)
				mcfg, _ := sck.getMigratingNewConfig()
				if mcfg == nil || mcfg.Num > new.Num {
					return
				}
				err = installShard(newclk, shardId, shard, new.Num)
			}
			sck.dlogln("installShard over")
			if err != rpc.OK {
				sck.dlogln("[changeConfigTo] InstallShard error: ", err, "shardId", shardId, "oldgid", oldgid, "newgid", newgid)
				return
			}
			sck.dlogln("deleteShard", shardId, "oldgid", oldgid, "new.Num", new.Num)
			err = deleteShard(oldclk, shardId, new.Num)
			for err == rpc.ErrgGroupMaybeLeave {
				sck.dlogln("deleteShard err shardId", shardId, "oldgid", oldgid, "new.Num", new.Num, err)
				mcfg, _ := sck.getMigratingNewConfig()
				if mcfg == nil || mcfg.Num > new.Num {
					return
				}
				err = deleteShard(oldclk, shardId, new.Num)
			}
			sck.dlogln("deleteShard over")
			if err != rpc.OK {
				sck.dlogln("[changeConfigTo] DeleteShard error: ", err, "shardId", shardId, "oldgid", oldgid, "newgid", newgid)
				return
			}
		}
	}
	// save new config
	err := sck.putConfig(new, oldVersion)
	if err != rpc.OK {
		sck.dlogln("[changeConfigTo] putConfig error:", err)
		return
	}
	sck.dlogln("[changeConfigTo] new config applied.", new)
}

// Return the current configuration
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	config, _ := sck.getConfig()
	return config
}

func (sck *ShardCtrler) getConfig() (*shardcfg.ShardConfig, rpc.Tversion) {
	configdata, version, err := sck.IKVClerk.Get(CONFIG_KEY)
	if err != rpc.OK && err != rpc.ErrNoKey {
		sck.dlogln("ShardCtrler getConfig RPC error:", err)
		return nil, rpc.Tversion(0)
	}
	config := shardcfg.FromString(configdata)
	return config, version
}

func (sck *ShardCtrler) putConfig(cfg *shardcfg.ShardConfig, oldVersion rpc.Tversion) rpc.Err {
	cfgdata := cfg.String()
	err := sck.IKVClerk.Put(CONFIG_KEY, cfgdata, oldVersion)
	if err != rpc.OK {
		sck.dlogln("ShardCtrler putConfig RPC error:", err)
		return err
	}
	return rpc.OK
}

// return rpc.OK if ok
func (sck *ShardCtrler) getMigratingNewConfig() (*shardcfg.ShardConfig, rpc.Tversion) {
	configdata, version, err := sck.IKVClerk.Get(CONFIG_MIGRATING_NEW_KEY)
	for err != rpc.OK && err != rpc.ErrNoKey {
		sck.dlogln("ShardCtrler getMigratingNewConfig RPC error:", err)
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
		sck.dlogln("ShardCtrler putMigratingNewConfig RPC error:", err)
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
		sck.dlogln("ShardCtrler putMigratingNewConfig RPC error:", err)
		return err
	}
	return rpc.OK
}

// freezeShard
func freezeShard(sgck *shardgrp.Clerk, s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	shard, err := sgck.FreezeShard(s, num)
	return shard, err
}

// installShard
func installShard(sgck *shardgrp.Clerk, s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	err := sgck.InstallShard(s, state, num)
	return err
}

// deleteShard
func deleteShard(sgck *shardgrp.Clerk, s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	err := sgck.DeleteShard(s, num)
	return err
}

func (sck *ShardCtrler) dlog(format string, a ...interface{}) {
	args := []any{
		sck.me,
	}
	args = append(args, a...)
	DPrintf("[Shradctrler %d] "+format, args...)
}

func (sck *ShardCtrler) dlogln(a ...interface{}) {
	args := []any{
		fmt.Sprintf("[Shradctrler %d]", sck.me),
	}
	args = append(args, a...)
	DPrintln(args...)
}
