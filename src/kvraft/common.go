package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	RequestID string
	Key       string
	Value     string
	Op        string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	RequestId string
	Err       Err
}

type GetArgs struct {
	RequestID string
	Key       string
	// You'll have to add definitions here.
}

type GetReply struct {
	RequestId string
	Err       Err
	Value     string
}
