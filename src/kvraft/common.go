package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrFailed      = "Failed to apply"
	ErrIgnored     = "ErrIgnored"
	ClientDebug    = true
	ServerDebug    = true
	TestDebug      = true
)

type Err string

// Put or Append
type PutAppendArgs struct {
	SeqId    int
	ClientId string
	Key      string
	Value    string
	Op       string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	SeqId    int
	ClientId string
	Err      Err
}

type GetArgs struct {
	SeqId    int
	ClientId string
	Key      string
	// You'll have to add definitions here.
}

type GetReply struct {
	SeqId    int
	ClientId string
	Err      Err
	Value    string
}

type Op struct {
	ClientId string
	SeqId    int
	OpType   OpType
	Key      string
	Value    string

	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type Reply struct {
	ServerId string
	ClientId string
	SeqId    int
	Result   interface{}
	Err      string
}

func (reply *Reply) CopyFrom(result Reply) {
	reply.Result = result.Result
	reply.Err = result.Err
	reply.SeqId = result.SeqId
	reply.ClientId = result.ClientId
	reply.ServerId = result.ServerId
}
