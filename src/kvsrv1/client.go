package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
	"time"
)

const RPC_RESEND_DURATION = 100 * time.Millisecond

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs {
		Key: key,
	}
	reply := rpc.GetReply{}
	ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
	for !ok {
		time.Sleep(RPC_RESEND_DURATION)
		// request or response is lost, it's safe to simply resend get
		ok = ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
	}
	return reply.Value, reply.Version, reply.Err
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := rpc.PutArgs {
		Key: key,
		Value: value,
		Version: version,
	}
	reply := rpc.PutReply{}
	ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
	for !ok {
		time.Sleep(RPC_RESEND_DURATION)
		// request or response is lost, it's safe to resend put with version
		ok = ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
		// however, there is a tricky case
		// if 1st request arrives in server, executes successfully, and response is lost
		// then we resend put and we will got ErrVersion (actually it's successfully!)
		// we cannot distinguish between this case and real ErrVersion (version changed by other client)
		if reply.Err == rpc.ErrVersion {
			reply.Err = rpc.ErrMaybe 
		}
	}
	return reply.Err
}
