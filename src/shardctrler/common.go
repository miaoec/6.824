package shardctrler

import (
	"sort"
)

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

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]

}

func (c *Config) ReBalance() {
	reShards := make([]int, 0)
	gMap := make(map[int][]int)
	//gNums := len(c.Groups)
	for idx, v := range c.Shards {
		if _, ok := gMap[v]; !ok {
			gMap[v] = []int{v}
		}
		gMap[v] = append(gMap[v], idx)
		if v == -1 {
			reShards = append(reShards, idx)
		}
	}
	gArr := make([][]int, len(gMap))
	for _, v := range gMap {
		gArr = append(gArr, v)
	}
	sort.Slice(
		gArr, func(i, j int) bool {
			return len(gArr[i]) > len(gArr[j])
		},
	)
	//max, min, f := math.Ceil(float64(NShards/gNums)), NShards/gNums, false
	//fmt.Println(max, min, f)
}

type Err string

type JoinArgs struct {
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard int
	GID   int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
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
