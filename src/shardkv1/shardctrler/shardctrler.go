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

	"fmt"
	"sync/atomic"
	"time"
)

const (
	CONFIG_KEY = "ShardCtrlerConfig"
	CONFIG_MIGRATING_NEW_KEY = "ShardCtrlerMigratingNewConfig"

	LEASE_KEY = "ShardCtrlerLease"
	LEASE_TIMEOUT = 100 * time.Millisecond

	MAX_RETRY_TIMES = 100
)

// ShardCtrler for the controller and kv clerk.
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk
	me int64
}

var sckCnt atomic.Int64

// Make a ShardCltler, which stores its state in a kvsrv.
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	sck.me = sckCnt.Add(1)
	// log.Println("[ShardCtrler] start up")
	return sck
}

func (sck *ShardCtrler) Id() int64 {
	return sck.me
}

func (sck *ShardCtrler) waitConcurrentCtrler() {

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
	donelease, failtokeep := sck.grabLease()

	var mcfgver rpc.Tversion
	var err error
	for {
		mcfgver, err = sck.checkAndCompleteMirgratingNewConfig()
		if err != nil {
			panic(err)
		}

		if !failtokeep.Load() {
			sck.grabLease()
		}

		sck.dlogln("start ChangeConfigTo")
		rerr := sck.putMigratingNewConfig(new, mcfgver)
		if rerr == rpc.ErrVersion || rerr == rpc.ErrMaybe {
			continue // retry until we publish migrating config successfully
		}
		if rerr != rpc.OK {
			panic(rerr)
		}
		mcfgver++
		break
	}

	if !failtokeep.Load() {
		sck.grabLease()
	}

	sck.changeConfigTo(new, false)

	if !failtokeep.Load() {
		sck.grabLease()
	}

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
	donelease <- struct{}{}
}

func (sck *ShardCtrler) checkAndCompleteMirgratingNewConfig() (rpc.Tversion, error) {
	// detect wheher exists an interrupted configuration mirgrating
	mcfg, mcfgver := sck.getMigratingNewConfig()
	if mcfg != nil {
		// recovery from an interrupted configuration mirgrating
		sck.dlogln("detect an interrupted configuration mirgrating, recovering")
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
	return mcfgver, nil
}


func (sck *ShardCtrler) changeConfigTo(new *shardcfg.ShardConfig, isrecover bool) {
	// migrate
	old, oldVersion := sck.getConfig()
	//log.Printf("[ChangeConfigTo] old %#v new %#v", old, new)
	for shardId := shardcfg.Tshid(0); shardId < shardcfg.NShards; shardId++ {
		oldgid, oldsrvs, ok := old.GidServers(shardId)
		if !ok {
			sck.dlogln("[ChangeConfigTo] get old.GidServers failed.", "shardId", shardId)
			return
		}
		newgid, newsrvs, ok := new.GidServers(shardId)
		if !ok {
			sck.dlogln("[ChangeConfigTo] get new.GidServers failed.", "shardId", shardId)
			return
		}
		if oldgid != newgid {
			sck.dlogln("moving shard from ", oldgid, " to ", newgid)
			oldclk, newclk := shardgrp.MakeClerk(sck.clnt, oldsrvs), shardgrp.MakeClerk(sck.clnt, newsrvs)
			sck.dlogln("freezeShard")
			shard, err := freezeShard(oldclk, shardId, new.Num)
			sck.dlogln("freezeShard over")
			if err != rpc.OK {
				if isrecover && err == rpc.ErrNoShard {
					sck.dlogln("[ChangeConfigTo] FreezeShard report noshard. maybe moving this shard has been completed by previous shardctrler.... continue. ", "shardId", shardId, "oldgid", oldgid, "newgid", newgid)
					continue // process next shard
				} else {
					sck.dlogln("[ChangeConfigTo] FreezeShard error: ", err, "shardId", shardId, "oldgid", oldgid, "newgid", newgid)
					return
				}
			}
			sck.dlogln("installShard")
			err = installShard(newclk, shardId, shard, new.Num)
			sck.dlogln("installShard over")
			if err != rpc.OK {
				sck.dlogln("[ChangeConfigTo] InstallShard error: ", err, "shardId", shardId, "oldgid", oldgid, "newgid", newgid)
				return
			}
			sck.dlogln("deleteShard")
			err = deleteShard(oldclk, shardId, new.Num)
			sck.dlogln("deleteShard over")
			if err != rpc.OK {
				sck.dlogln("[ChangeConfigTo] DeleteShard error: ", err, "shardId", shardId, "oldgid", oldgid, "newgid", newgid)
				return
			}
		}
	}
	// save new config
	err := sck.putConfig(new, oldVersion)
	if err != rpc.OK {
		sck.dlogln("[ChangeConfigTo] putConfig error:", err)
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
		sck.dlogln("ShardCtrler getConfig RPC error: %v", err)
		return nil, rpc.Tversion(0)
	}
	config := shardcfg.FromString(configdata)
	return config, version
}

func (sck *ShardCtrler) putConfig(cfg *shardcfg.ShardConfig, oldVersion rpc.Tversion) rpc.Err {
	cfgdata := cfg.String()
	err := sck.IKVClerk.Put(CONFIG_KEY, cfgdata, oldVersion)
	if err != rpc.OK {
		sck.dlogln("ShardCtrler putConfig RPC error: %v", err)
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

func (sck *ShardCtrler) setLease(oldVersion rpc.Tversion) rpc.Err {
	err := sck.IKVClerk.Put(LEASE_KEY, "true", oldVersion)
	// if err != rpc.OK && err != rpc.ErrNoKey {
	// 	sck.dlogln("ShardCtrler getConfig RPC error: %v", err)
	// 	panic(err)
	// }
	return err
}


func (sck *ShardCtrler) waitLease() rpc.Tversion {
	checkLease := func() (bool, rpc.Tversion) {
		content, version, err := sck.IKVClerk.Get(LEASE_KEY)
		if err != rpc.OK && err != rpc.ErrNoKey {
			sck.dlogln("ShardCtrler getConfig RPC error: %v", err)
			panic(err)
		}
		if err == rpc.ErrNoKey {
			return false, rpc.Tversion(0)
		}
		return content!="", version
	}

	exist, ver := checkLease()
	for exist {
		time.Sleep(2 * LEASE_TIMEOUT)
		exist2, ver2 := checkLease()
		if exist2 {
			if ver == ver2 {
				// expired
				return ver2
			} else {
				// continue to wait
				ver = ver2
			}
		} else {
			// ok...
			return ver2
		}
	}
	return ver
}

func (sck *ShardCtrler) contendLease(ver rpc.Tversion) bool {
	err := sck.setLease(ver)
	if err == rpc.ErrVersion || err == rpc.ErrMaybe {
		return false
	}
	sck.dlog("contendLease succ num:%v", ver+1)
	return true
}

func (sck *ShardCtrler) keepLease(ver rpc.Tversion) (chan<- struct{}, *atomic.Bool) {
	done := make(chan struct{}, 1)
	failtokeep := &atomic.Bool{}
	go func() {
		for {
			select {
			case <-done:
				sck.dlog("doneLease...")
				sck.IKVClerk.Put(LEASE_KEY, "", ver)
				return
			default:
				sck.dlog("keepLease...")
				err := sck.setLease(ver)
				if err == rpc.ErrVersion || err == rpc.ErrMaybe {
					sck.dlog("failtokeep!")
					failtokeep.Store(true)
					return
				}
				ver++
				time.Sleep(LEASE_TIMEOUT)
			}
		}
	}()
	return done, failtokeep
}

func (sck *ShardCtrler) grabLease() (chan<- struct{}, *atomic.Bool) {
	ownLease := false
	var ver rpc.Tversion
	for !ownLease {
		sck.dlog("waitLease...")
		ver = sck.waitLease()
		sck.dlog("contendLease!")
		ownLease = sck.contendLease(ver)
	}
	return sck.keepLease(ver)
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
// retry while ErrgGroupMaybeLeave
//
// note: we cannot distinguish between
//
// requests/responses lost due to unreliable networks
// &
// the target shardgroup servers being offline/shutdown.
// So if the target shardgroup servers is offline and can never respond, this function will never return.
func freezeShard(sgck *shardgrp.Clerk, s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	shard, err := sgck.FreezeShard(s, num)
	retry := 0
	for err == rpc.ErrgGroupMaybeLeave && retry < MAX_RETRY_TIMES {
		shard, err = sgck.FreezeShard(s, num)
		retry++
	}
	return shard, err
}

// installShard
// retry while ErrgGroupMaybeLeave
//
// note: we cannot distinguish between
//
// requests/responses lost due to unreliable networks
// &
// the target shardgroup servers being offline/shutdown.
// So if the target shardgroup servers is offline and can never respond, this function will never return.
func installShard(sgck *shardgrp.Clerk, s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	err := sgck.InstallShard(s, state, num)
	retry := 0
	for err == rpc.ErrgGroupMaybeLeave && retry < MAX_RETRY_TIMES {
		err = sgck.InstallShard(s, state, num)
		retry++
	}
	return err
}

// deleteShard
// retry while ErrgGroupMaybeLeave
//
// note: we cannot distinguish between
//
// requests/responses lost due to unreliable networks
// &
// the target shardgroup servers being offline/shutdown.
// So if the target shardgroup servers is offline and can never respond, this function will never return.
func deleteShard(sgck *shardgrp.Clerk, s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	err := sgck.DeleteShard(s, num)
	retry := 0
	for err == rpc.ErrgGroupMaybeLeave && retry < MAX_RETRY_TIMES {
		err = sgck.DeleteShard(s, num)
		retry++
	}
	return err
}

func (sck *ShardCtrler) dlog(format string, a ...interface{}) {
	args := []any{
		sck.me,
	}
	args = append(args, a...)
	DPrintf("[Shradctrler %d] " + format, args...)
}


func (sck *ShardCtrler) dlogln(a ...interface{}) {
	args := []any{
		fmt.Sprintf("[Shradctrler %d]", sck.me),
	}
	args = append(args, a...)
	DPrintln(args...)
}
