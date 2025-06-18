package lock

import (
	"6.5840/kvtest1"
	"6.5840/kvsrv1/rpc"
	"time"
)

const RETRY_DURATION = 100 * time.Millisecond

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockKey string
	clientID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	// You may add code here
	lk.lockKey = l
	lk.clientID = kvtest.RandValue(8)
	return lk
}


func (lk *Lock) Acquire() {
	// Your code here
	holding_client, lockVer, err := lk.ck.Get(lk.lockKey)
	if err == rpc.ErrNoKey {
		// try to create key and obtain the lock
		puterr := lk.ck.Put(lk.lockKey, lk.clientID, 0)
		if puterr == rpc.OK {
			return // okay
		}
		if puterr == rpc.ErrMaybe {
			// we dont know succ or fail...
			// it doesn't matter, we reget to see who is holding
		} else if puterr != rpc.ErrVersion {
			panic(puterr)
		}
		// other client have created, the lock maybe holding or already released
		// or we created and obtained but got ErrMaybe
		// so we reget
		holding_client, lockVer, err = lk.ck.Get(lk.lockKey)
		if err != rpc.OK {
			panic(puterr)
		}
	}
	for {
		// check holding client
		if holding_client == lk.clientID {
			// maybe duplicated acquire or previous ErrMaybe put
			return
		}
		for holding_client != "" {
			time.Sleep(RETRY_DURATION)
			// waiting for releasing
			holding_client, lockVer, err = lk.ck.Get(lk.lockKey)
			if err != rpc.OK {
				panic(err)
			}
		}
		// released, try to obtain the lock
		puterr := lk.ck.Put(lk.lockKey, lk.clientID, lockVer)
		if puterr == rpc.OK {
			return // okay
		} else if puterr == rpc.ErrMaybe {
			// we dont know succ or fail...
			holding_client, lockVer, err = lk.ck.Get(lk.lockKey) // reget to see holding_client
		} else if puterr == rpc.ErrVersion {
			holding_client, lockVer, err = lk.ck.Get(lk.lockKey) // contending, retry in next round
		} else {
			panic(puterr)
		}
	}
}

// if Acquire() never called, calling to Release() will panics
// if the client is not holding the lock, this opertion will be no-op
func (lk *Lock) Release() {
	// Your code here
	holding_client, lockVer, err := lk.ck.Get(lk.lockKey)
	if err != rpc.OK {
		panic(err)
	}
	// check holding client
	if holding_client != lk.clientID {
		return
	}
	// release the lock
	err = lk.ck.Put(lk.lockKey, "", lockVer)
	if err == rpc.ErrMaybe {
		// the put must have been performed, because we have observed holding_client == lk.clientID then only this client will override it
		return
	}
	if err != rpc.OK {
		// ErrNoKey is impossible, because we have observed a version
		// ErrVersion is impossible, because we have observed holding_client == lk.clientID then only this client will override it
		panic(err)
	}
}
