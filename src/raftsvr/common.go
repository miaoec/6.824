package raftsvr

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

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

type Op struct {
	ClientId string
	SeqId    int
	OpType   OpType
	Key      string
	Value    interface{}

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
