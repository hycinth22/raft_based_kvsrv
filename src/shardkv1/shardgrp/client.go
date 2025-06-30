package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	"6.5840/tester1"
)

const RPC_RESEND_DURATION = 1 * time.Millisecond

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	lastSeenLeader int
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{clnt: clnt, servers: servers, lastSeenLeader: 0}
	return ck
}

func (ck *Clerk) requestLeader(requestOp func(tryServerIdx int) (successReq bool, data any) ) (reply any) {
	var ok bool
	leader := ck.lastSeenLeader
	if ok, reply = requestOp(leader); ok {
		return reply
	}
	ck.dlog("[requestLeader] request sending to %v failed. retry to find the leader", leader)
	for !ok {
		leader = (leader + 1) % len(ck.servers)
		if leader == ck.lastSeenLeader {
			return nil
		}
		time.Sleep(RPC_RESEND_DURATION)

		// resend
		ck.dlog("[requestLeader] resend to %v", leader)
		ok, reply = requestOp(leader)
	}
	ck.lastSeenLeader = leader
	ck.dlog("[requestLeader] the leader is %v now", leader)
	return reply
}

// Get fetches the current value and version for a key.
//
// It returns OK if Get succeeds.
// It returns ErrNoKey if the key does not exist.
// It keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs {
		Key: key,
	}
	xreply := ck.requestLeader(func(tryServerIdx int) (bool, any) {
		reply := rpc.GetReply{}
		ck.dlog("[Get] (args %#v)send to server %v", args, tryServerIdx)
		ok := ck.clnt.Call(ck.servers[tryServerIdx], "KVServer.Get", &args, &reply)
		if !ok {
			ck.dlog("[Get] failed. (args %#v) server %v, try next server", args, tryServerIdx)
			return false, reply
		}
		if reply.Err != rpc.ErrWrongGroup && reply.Err != rpc.OK && reply.Err != rpc.ErrNoKey {
			ck.dlog("[Get] ok but err undesired. (args %#v) server %v reply %v", args, tryServerIdx, reply)
			return false, reply
		}
		return true, reply
	})
	if xreply == nil {
		return "", 0, rpc.ErrgGroupMaybeLeave
	}
	reply := xreply.(rpc.GetReply)
	ck.dlog("[Get] resolved. (args %#v) finalreply %v", args, reply)
	return reply.Value, reply.Version, reply.Err
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.
//
// It returns OK if Put succeeds.
// It returns ErrNoKey if the key does not exist and argument version > 0
// It returns ErrVersion if the version does not match
// It returns ErrMaybe when Put maybe have been processed.
// It keeps trying forever in the face of all other errors.
//
// Explanation for ErrMaybe:
// If Put receives an ErrVersion on its first RPC, Put return ErrVersion,
// since the Put was definitely not performed at the server.
// If the server returns ErrVersion on a resend RPC,
// then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs {
		Key: key,
		Value: value,
		Version: version,
	}
	failSeen := false
	xreply := ck.requestLeader(func(tryServerIdx int) (bool, any) {
		reply := rpc.PutReply{}
		ck.dlog("[Put] (args %#v) will send to server %v", args, tryServerIdx)
		ok := ck.clnt.Call(ck.servers[tryServerIdx], "KVServer.Put", &args, &reply)
		if !ok {
			ck.dlog("[Put] failed. (args %#v) server %v, try next server", args, tryServerIdx)
			failSeen = true
			return false, reply
		}
		if reply.Err != rpc.ErrWrongGroup && reply.Err != rpc.OK && reply.Err != rpc.ErrNoKey && reply.Err != rpc.ErrVersion {
			ck.dlog("[Put] ok but err undesired. (args %#v) server %v reply %v", args, tryServerIdx, reply)
			return false, reply
		}
		if failSeen {
			// however, there is a tricky case
			// if 1st request arrives in server, executes successfully, and response is lost
			// then we resend put and we will got ErrVersion (actually it's successfully!)
			// we cannot distinguish between this case and real ErrVersion (version changed by other client)
			if reply.Err == rpc.ErrVersion {
				ck.dlog("[Put] set ErrMaybe due to maybe previous response lost. (args %#v) server %v reply %v", args, tryServerIdx, reply)
				reply.Err = rpc.ErrMaybe
			}
		}
		return true, reply
	})
	if xreply == nil {
		return rpc.ErrgGroupMaybeLeave
	}
	reply := xreply.(rpc.PutReply)
	ck.dlog("[Put] resolved. (args %#v) finalreply %v", args, reply)
	return reply.Err
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := shardrpc.FreezeShardArgs {
		Shard: s,
		Num: num,
	}
	failSeen := false
	xreply := ck.requestLeader(func(tryServerIdx int) (bool, any) {
		reply := shardrpc.FreezeShardReply{}
		ck.dlog("[FreezeShard] (args %#v) will send to server %v", args, tryServerIdx)
		ok := ck.clnt.Call(ck.servers[tryServerIdx], "KVServer.FreezeShard", &args, &reply)
		if !ok {
			ck.dlog("[FreezeShard] failed. (args %#v) server %v, try next server", args, tryServerIdx)
			failSeen = true
			return false, reply
		}
		if reply.Err != rpc.OK && reply.Err != rpc.ErrVersion && reply.Err != rpc.ErrNoShard {
			ck.dlog("[FreezeShard] ok but err undesired. (args %#v) server %v reply %v", args, tryServerIdx, reply)
			return false, reply
		}
		if failSeen {
			// however, there is a tricky case
			// if 1st request arrives in server, executes successfully, and response is lost
			// then we resend put and we will got ErrVersion (actually it's successfully!)
			// we cannot distinguish between this case and real ErrVersion (version changed by other client)
			if reply.Err == rpc.ErrVersion {
				ck.dlog("[FreezeShard] set ErrMaybe due to maybe previous response lost. (args %#v) server %v reply %v", args, tryServerIdx, reply)
				reply.Err = rpc.ErrMaybe
			}
		}
		return true, reply
	})
	if xreply == nil {
		return nil, rpc.ErrgGroupMaybeLeave
	}
	reply := xreply.(shardrpc.FreezeShardReply)
	ck.dlog("[FreezeShard] resolved. (args %#v) finalreply %v", args, reply)
	return reply.State, reply.Err
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.InstallShardArgs {
		Shard: s,
		State: state,
		Num:   num,
	}
	failSeen := false
	xreply := ck.requestLeader(func(tryServerIdx int) (bool, any) {
		reply := shardrpc.InstallShardReply{}
		ck.dlog("[InstallShard] (args %#v) will send to server %v", args, tryServerIdx)
		ok := ck.clnt.Call(ck.servers[tryServerIdx], "KVServer.InstallShard", &args, &reply)
		if !ok {
			ck.dlog("[InstallShard] failed. (args %#v) server %v, try next server", args, tryServerIdx)
			failSeen = true
			return false, reply
		}
		if reply.Err != rpc.OK && reply.Err != rpc.ErrVersion && reply.Err != rpc.ErrExistShard && reply.Err != rpc.ErrInvalidShardState {
			ck.dlog("[InstallShard] ok but err undesired. (args %#v) server %v reply %v", args, tryServerIdx, reply)
			return false, reply
		}
		if failSeen {
			// however, there is a tricky case
			// if 1st request arrives in server, executes successfully, and response is lost
			// then we resend put and we will got ErrExistShard (actually it's successfully!)
			// we cannot distinguish between this case and real ErrVersion (version changed by other client)
			if reply.Err == rpc.ErrVersion || reply.Err == rpc.ErrExistShard {
				ck.dlog("[InstallShard] set ErrMaybe due to maybe previous response lost. (args %#v) server %v reply %v", args, tryServerIdx, reply)
				reply.Err = rpc.ErrMaybe
			}
		}
		return true, reply
	})
	if xreply == nil {
		return rpc.ErrgGroupMaybeLeave
	}
	reply := xreply.(shardrpc.InstallShardReply)
	ck.dlog("[InstallShard] resolved. (args %#v) finalreply %v", args, reply)
	return reply.Err
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs {
		Shard: s,
		Num: num,
	}
	failSeen := false
	xreply := ck.requestLeader(func(tryServerIdx int) (bool, any) {
		reply := shardrpc.DeleteShardReply{}
		ck.dlog("[DeleteShard] (args %#v) will send to server %v", args, tryServerIdx)
		ok := ck.clnt.Call(ck.servers[tryServerIdx], "KVServer.DeleteShard", &args, &reply)
		if !ok {
			ck.dlog("[DeleteShard] failed. (args %#v) server %v, try next server", args, tryServerIdx)
			failSeen = true
			return false, reply
		}
		if reply.Err != rpc.OK && reply.Err != rpc.ErrVersion && reply.Err != rpc.ErrNoShard {
			ck.dlog("[DeleteShard] ok but err undesired. (args %#v) server %v reply %v", args, tryServerIdx, reply)
			return false, reply
		}
		if failSeen {
			// however, there is a tricky case
			// if 1st request arrives in server, executes successfully, and response is lost
			// then we resend put and we will got ErrNoShard (actually it's successfully!)
			// we cannot distinguish between this case and real ErrVersion (version changed by other client)
			if reply.Err == rpc.ErrVersion || reply.Err == rpc.ErrNoShard {
				ck.dlog("[DeleteShard] set ErrMaybe due to maybe previous response lost. (args %#v) server %v reply %v", args, tryServerIdx, reply)
				reply.Err = rpc.ErrMaybe
			}
		}
		return true, reply
	})
	if xreply == nil {
		return rpc.ErrgGroupMaybeLeave
	}
	reply := xreply.(shardrpc.DeleteShardReply)
	ck.dlog("[DeleteShard] resolved. (args %#v) finalreply %v", args, reply)
	return reply.Err
}

func (ck *Clerk) dlog(format string, args ...interface{}) {
	DPrintf("[KVClerk] " + format, args...)
}
