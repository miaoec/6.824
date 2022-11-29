package kvraft

import (
	"6.824/labrpc"
	"fmt"
	"github.com/google/uuid"
	"log"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId int
	cmds     []interface{}
}

const isDebug = false

func (ck *Clerk) log(str string, args ...interface{}) {
	if isDebug {
		log.Printf(
			fmt.Sprintf(
				"Ck(id:%v)##: %v", ck.leaderId,
				str,
			),
			args...,
		)
	}

}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func (ck *Clerk) checkoutLeaderId(leaderId int) {
	ck.leaderId = leaderId % len(ck.servers)
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		RequestID: uuid.NewString()[0:10],
		Key:       key,
	}
	ck.cmds = append(ck.cmds, args)
	ck.log("Get:%+v", args)
	for {
		ck.log("try send Get%+v, to %v", args, ck.leaderId)
		reply := GetReply{RequestId: args.RequestID}
		if ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply) {
			ck.log("Get reply%+v", reply)
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrWrongLeader {
				ck.checkoutLeaderId(ck.leaderId + 1)
			} else if reply.Err == ErrNoKey {
				return ""
			}
		} else {
			ck.log("Get reply failed")
			ck.checkoutLeaderId(ck.leaderId + 1)
		}
		//time.Sleep(500 * time.Millisecond)
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//ck.servers.
	args := PutAppendArgs{
		RequestID: uuid.NewString()[0:10],
		Op:        op,
		Key:       key,
		Value:     value,
	}
	ck.cmds = append(ck.cmds, args)
	ck.log("PutAppend:%+v", args)
	for {
		reply := PutAppendReply{RequestId: args.RequestID}
		ck.log("try send PutAppend%+v, to %v", args, ck.leaderId)
		if ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply) {
			ck.log("PutAppend reply%+v", reply)
			if reply.Err == OK {
				return
			} else if reply.Err == ErrWrongLeader {
				ck.checkoutLeaderId(ck.leaderId + 1)
			}
		} else {
			ck.checkoutLeaderId(ck.leaderId + 1)
		}
		//time.Sleep(500 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {

	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
