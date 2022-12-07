package shardkv

import "6.824/shardctrler"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
	ErrFailed      = "Failed to apply"
	ErrIgnored     = "ErrIgnored"
	ClientDebug    = true
	ServerDebug    = true
	TestDebug      = false

	PUT          OpType = "Put"
	GET          OpType = "Get"
	APPEND       OpType = "Append"
	ChangeConfig OpType = "ChangeConfig"

	LogCommand    CommandType = "LogCommand"
	ConfigCommand CommandType = "ConfigCommand"
	ShardCommand  CommandType = "ShardCommand"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}

type Command struct {
	CommandType   CommandType
	LogCommand    Op
	ConfigCommand shardctrler.Config
	ShardCommand  ShardData
}

type ShardData struct {
	ShardKey int
	Shard    Shard
}

type Op struct {
	ClientId string
	SeqId    int
	OpType   OpType
	Key      string
	Value    string
}

func (op *Op) deepCopy() Op {
	return Op{
		ClientId: op.ClientId,
		SeqId:    op.SeqId,
		OpType:   op.OpType,
		Key:      op.Key,
		Value:    op.Value,
	}
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
