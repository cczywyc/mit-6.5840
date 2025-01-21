package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu         sync.Mutex
	store      map[string]string
	clientSeqs map[int64]int // to track the latest sequence number for each client
}

// Get fetches the current value for the key.
// A Get for a non-existing key should return an empty string.
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	value, exists := kv.store[args.Key]
	if !exists {
		reply.Value = ""
	} else {
		reply.Value = value
	}
}

// Put installs or replaces the value for a particular key in the map.
func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.SeqNumber <= kv.clientSeqs[args.ClientId] {
		reply.PreviousValue = kv.store[args.Key]
		return
	}

	kv.store[args.Key] = args.Value
	reply.PreviousValue = kv.store[args.Key]

	// update the request sequence number for the client
	kv.clientSeqs[args.ClientId] = args.SeqNumber
}

// Append appends arg to key's value and returns the old value.
// An Append to a non-existing key should act as if the existing value were a zero-length string.
func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.SeqNumber <= kv.clientSeqs[args.ClientId] {
		reply.PreviousValue = kv.store[args.Key]
		return
	}

	// find the key if exist
	value, exists := kv.store[args.Key]
	if !exists {
		// a zero-length string
		value = ""
	}
	// append the value
	kv.store[args.Key] = value + args.Value
	reply.PreviousValue = value

	// update the request sequence number for the client
	kv.clientSeqs[args.ClientId] = args.SeqNumber
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	// init the args
	kv.store = make(map[string]string)
	kv.clientSeqs = make(map[int64]int)
	return kv
}
