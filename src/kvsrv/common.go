package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string // Op type, "Put" or "Append"
	ClientId  int64
	SeqNumber int
}

// PutAppendReply : lab introduction: append appends arg to key's value and returns the old value
type PutAppendReply struct {
	PreviousValue string
}

type GetArgs struct {
	Key       string
	ClientId  int64
	SeqNumber int
}

type GetReply struct {
	Value string
}
