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

func (c *Config) ReBalance(sc *ShardCtrler, opts map[int]int) {
	reShards := make([]int, 0)
	gMap := make(map[int][]int)
	gNums := len(c.Groups)
	for idx, v := range c.Shards {
		if _, ok := c.Groups[v]; !ok {
			c.Shards[idx] = -1
		}
	}
	for i, _ := range c.Groups {
		if _, ok := gMap[i]; !ok {
			gMap[i] = []int{}
		}
	}
	for idx, v := range c.Shards {
		if c.Shards[idx] >= 0 {
			gMap[v] = append(gMap[v], idx)
		} else {
			reShards = append(reShards, idx)
		}
	}
	var gArr [][]int
	for idx, v := range gMap {
		gArr = append(gArr, append([]int{idx}, v...))
	}
	leftG := gNums
	leftS := NShards
	sc.log("reBalance2:gnums%v,%+v\n", gNums, gArr)
	//这里先从大到小排序，然后尽量处于平衡,要保证排序结果不受map 遍历随机影响
	sorF := func(i, j int) bool {
		if len(gArr[i]) == len(gArr[j]) {
			return gArr[i][0] > gArr[j][0]
		}
		return len(gArr[i]) > len(gArr[j])
	}
	sort.Slice(gArr, sorF)
	for k, _ := range gArr {
		for idx := 1; (len(gArr[k])-1)*(leftG) > leftS && idx < len(gArr[k]); idx++ {
			if _, ok := opts[gArr[k][idx]]; !ok {
				reShards = append(reShards, gArr[k][idx])
				c.Shards[gArr[k][idx]] = -1
				gArr[k] = append(gArr[k][:idx], gArr[k][idx+1:]...)
				idx--
			}
		}
		leftS -= len(gArr[k]) - 1
		leftG--
	}
	sort.Slice(gArr, sorF)
	reshardIdx := 0
	leftG = gNums
	leftS = NShards
	sc.log("reBalance:gnums%v,%+v", gNums, gArr)
	//fmt.Printlf("")
	for i := range gArr {
		for ; (len(gArr[i])-1)*(leftG) < leftS && reshardIdx < len(reShards); reshardIdx++ {
			gArr[i] = append(gArr[i], reShards[reshardIdx])
			c.Shards[reShards[reshardIdx]] = gArr[i][0]
		}
		leftG -= 1
		leftS -= len(gArr[i]) - 1
	}
	sc.log("reBalance:%+v", gArr)
	sc.lastBan = c
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
