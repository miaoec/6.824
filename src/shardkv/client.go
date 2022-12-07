package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"6.824/labrpc"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"io"
	"log"
	"os"
)
import "crypto/rand"
import "math/big"
import "6.824/shardctrler"
import "time"

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm          *shardctrler.Clerk
	config      shardctrler.Config
	make_end    func(string) *labrpc.ClientEnd
	seq         int
	configSeqId int
	clientID    string

	cmdIn io.Writer
	cmds  []interface{}
	// You will have to modify this struct.
}

//
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.clientID = uuid.NewString()[0:6]
	if ClientDebug {
		ck.cmdIn, _ = os.Create(ck.clientID + ".client.log")
	}
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//

func (ck *Clerk) cmd(op interface{}) {
	ck.cmds = append(ck.cmds, op)
	by, _ := json.Marshal(op)
	if ClientDebug {
		fmt.Fprintf(ck.cmdIn, "%+v\n", string(by))
	}
}

func (ck *Clerk) log(str string, args ...interface{}) {
	if ClientDebug {
		log.Printf(
			fmt.Sprintf(
				"Ck(id:%v)##: %v", ck.clientID,
				str,
			),
			args...,
		)
	}

}
func (ck *Clerk) doOp(op *Op, reply *Reply) {
	ck.seq++
	op.SeqId = ck.seq
	op.ClientId = ck.clientID
	ck.cmd(*op)
	defer ck.cmd(*reply)
	for {
		shard := key2shard(op.Key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				ck.log("try send to %v-%v %+v", gid, si, op)
				srv := ck.make_end(servers[si])
				ok := srv.Call("ShardKV.Do", op, reply)
				ck.log("doreply %+v", reply)
				if ok && reply.Err == OK || reply.Err == ErrIgnored || reply.Err == ErrNoKey {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
		ck.log("sync config,%+v", ck.config)
	}

}

func (ck *Clerk) Get(key string) string {
	op := Op{
		OpType: GET,
		Key:    key,
	}
	reply := Reply{}
	ck.doOp(&op, &reply)
	if v, ok := reply.Result.(string); ok {
		return v
	} else {
		return ""
	}
}

func (ck *Clerk) Put(key string, value string) {
	//ck.PutAppend(key, value, "Put")
	op := Op{
		OpType: PUT,
		Key:    key,
		Value:  value,
	}
	reply := Reply{}
	ck.doOp(&op, &reply)
}
func (ck *Clerk) Append(key string, value string) {
	//ck.PutAppend(key, value, "Append")
	op := Op{
		OpType: APPEND,
		Key:    key,
		Value:  value,
	}
	reply := Reply{}
	ck.doOp(&op, &reply)
}
