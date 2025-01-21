package kvsrv

import (
	"crypto/rand"
	"github.com/cczywyc/mit-6.5840/src/labrpc"
	"math/big"
)

type Clerk struct {
	server    *labrpc.ClientEnd
	clientId  int64
	seqNumber int
}

func nrand() int64 {
	maxI := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, maxI)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.clientId = nrand()
	ck.seqNumber = 0
	return ck
}

// Get fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		SeqNumber: ck.seqNumber,
	}
	reply := GetReply{}
	ck.seqNumber++

	// send the Get RPC request
	ok := ck.server.Call("KVServer.Get", &args, &reply)
	if ok {
		return reply.Value
	}
	return ""
}

// PutAppend shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		SeqNumber: ck.seqNumber,
	}
	reply := PutAppendReply{}
	ck.seqNumber++

	// send the RPC request
	ok := ck.server.Call("KVServer."+op, &args, &reply)
	if ok {
		return reply.PreviousValue
	}
	return ""
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
