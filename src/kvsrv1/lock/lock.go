package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"encoding/json"
	"strings"
	"time"
)

const RETRY_DURATION = 100 * time.Millisecond
const ErrLockReleased rpc.Err = "LockReleased"

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.
	// passes the clerk in when calling MakeLock().
	ck kvtest.IKVClerk

	lockKey  string
	clientID string

	timeout  time.Duration
}

// calls MakeLock() and passes in a k/v clerk
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck}
	lk.lockKey = l
	lk.clientID = kvtest.RandValue(8)
	lk.timeout = time.Until(time.Now().AddDate(100, 0, 0))
	return lk
}

func MakeExpiredLock(ck kvtest.IKVClerk, l string, timeout time.Duration) *Lock {
	lk := MakeLock(ck, l)
	lk.timeout = timeout
	return lk
}

func (lk *Lock) TryAcquire() bool {
	lockVal, lockVer, err := lk.ck.Get(lk.lockKey)
	if err == rpc.ErrNoKey {
		// try to create key and obtain the lock
		puterr := lk.ck.Put(lk.lockKey, lk.genLockVal(), 0)
		if puterr == rpc.OK {
			return true // okay
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
		lockVal, lockVer, err = lk.ck.Get(lk.lockKey)
		if err != rpc.OK {
			panic(err)
		}
	}
	// check holding client
	v := parseLockVal(lockVal)
	if v.HoldingClientID == lk.clientID {
		return true
	}
	time.Sleep(RETRY_DURATION)
	lockVal, lockVer, err = lk.ck.Get(lk.lockKey)
	if err != rpc.OK {
		panic(err)
	}
	// released, try to obtain the lock
	puterr := lk.ck.Put(lk.lockKey, lk.genLockVal(), lockVer)
	if puterr == rpc.OK {
		return true // okay
	} else if puterr == rpc.ErrMaybe {
		// we dont know succ or fail...
		lockVal, lockVer, err = lk.ck.Get(lk.lockKey) // reget to see holding_client
	} else if puterr == rpc.ErrVersion {
		lockVal, lockVer, err = lk.ck.Get(lk.lockKey) // contending, retry in next round
	} else {
		panic(puterr)
	}
	lockVal, lockVer, err = lk.ck.Get(lk.lockKey)
	if err != rpc.OK {
		panic(err)
	}
	v = parseLockVal(lockVal)
	return v.HoldingClientID == lk.clientID
}

func (lk *Lock) Acquire() {
	lockVal, lockVer, err := lk.ck.Get(lk.lockKey)
	if err == rpc.ErrNoKey {
		// try to create key and obtain the lock
		puterr := lk.ck.Put(lk.lockKey, lk.genLockVal(), 0)
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
		lockVal, lockVer, err = lk.ck.Get(lk.lockKey)
		if err != rpc.OK {
			panic(err)
		}
	}
	for {
		// check holding client
		v := parseLockVal(lockVal)
		if v.HoldingClientID == lk.clientID {
			// maybe duplicated acquire or previous ErrMaybe put
			return
		}
		for lockVal != "" {
			if time.Now().After(v.ExpiredAt) {
				break
			}
			time.Sleep(RETRY_DURATION)
			// waiting for releasing
			lockVal, lockVer, err = lk.ck.Get(lk.lockKey)
			if err != rpc.OK {
				panic(err)
			}
		}
		// released, try to obtain the lock
		puterr := lk.ck.Put(lk.lockKey, lk.genLockVal(), lockVer)
		if puterr == rpc.OK {
			return // okay
		} else if puterr == rpc.ErrMaybe {
			// we dont know succ or fail...
			lockVal, lockVer, err = lk.ck.Get(lk.lockKey) // reget to see holding_client
		} else if puterr == rpc.ErrVersion {
			lockVal, lockVer, err = lk.ck.Get(lk.lockKey) // contending, retry in next round
		} else {
			panic(puterr)
		}
	}
}

// if Acquire() never called, calling to Release() will panics
// if the client is not holding the lock, this opertion will be no-op
func (lk *Lock) Release() {
	lockVal, lockVer, err := lk.ck.Get(lk.lockKey)
	if err != rpc.OK {
		panic(err)
	}
	if lockVal == "" {
		return
	}
	// check holding client
	v := parseLockVal(lockVal)
	if v.HoldingClientID != lk.clientID {
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

func (lk *Lock) LockedByOther() bool {
	lockVal, _, err := lk.ck.Get(lk.lockKey)
	if err == rpc.ErrNoKey {
		return false
	}
	if err != rpc.OK {
		panic(err)
	}
	v := parseLockVal(lockVal)
	return v.HoldingClientID != lk.clientID || time.Now().After(v.ExpiredAt)
}

func (lk *Lock) ExtendExpiration() bool {
	lockVal, lockVer, err := lk.ck.Get(lk.lockKey)
	if err != rpc.OK {
		return false
	}
	v := parseLockVal(lockVal)
	if v.HoldingClientID != lk.clientID {
		return false
	}
	err = lk.ck.Put(lk.lockKey, lk.genLockVal(), lockVer)
	for err == rpc.ErrMaybe {
		lockVal, lockVer, err = lk.ck.Get(lk.lockKey)
		v := parseLockVal(lockVal)
		if v.HoldingClientID != lk.clientID {
			return false
		}
		err = lk.ck.Put(lk.lockKey, lk.genLockVal(), lockVer)
	}
	if err == rpc.ErrVersion {
		return false
	}
	if err != rpc.OK {
		panic(err)
	}
	return true
}

type lockValue struct {
	HoldingClientID string
	ExpiredAt       time.Time
}

func (lk *Lock) genLockVal() string {
	var buffer strings.Builder
	enc := json.NewEncoder(&buffer)
	if err := enc.Encode(lockValue{
		HoldingClientID: lk.clientID,
		ExpiredAt:       time.Now().Add(lk.timeout),
	}); err != nil {
		panic(err)
	}
	return buffer.String()
}

func parseLockVal(s string) lockValue {
	if len(s) < 1 {
		return lockValue{}
	}
	dec := json.NewDecoder(strings.NewReader(s))
	var val lockValue
	if err := dec.Decode(&val); err != nil {
		panic(err)
	}
	return val
}
