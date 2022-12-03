package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.824/labrpc"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"io"
	"log"
	"os"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	sync.Mutex
	servers []*labrpc.ClientEnd
	// Your data here.
	leaderId int
	cmds     []interface{}
	seq      int
	clientID string
	cmdIn    io.Writer
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.seq = 1
	ck.clientID = uuid.NewString()[0:6]
	ck.cmdIn, _ = os.Create(ck.clientID + ".client.log")
	return ck
}

func (ck *Clerk) log(str string, args ...interface{}) {
	if ClientDebug {
		log.Printf(
			fmt.Sprintf(
				"Ck(id:%v,leto:%v)##: %v", ck.clientID, ck.leaderId,
				str,
			),
			args...,
		)
	}
}
func (ck *Clerk) cmd(op Op) {
	ck.cmds = append(ck.cmds, op)
	by, _ := json.Marshal(op)
	fmt.Fprintf(ck.cmdIn, "%+v\n", string(by))
}

func (ck *Clerk) checkoutLeaderId(leaderId int) {
	ck.leaderId = leaderId % len(ck.servers)
}
func (ck *Clerk) doOp(op *Op, reply *Reply) {
	ck.cmd(*op)
	ck.Lock()
	defer ck.Unlock()
	ck.seq++
	for {
		ck.log("try send Req%+v, to %v", op, ck.leaderId)
		if ck.servers[ck.leaderId].Call("ShardCtrler.Do", op, reply) {
			ck.log("to%v, do reply%+v", ck.leaderId, reply)
			if reply.Err == OK || reply.Err == ErrIgnored {
				return
			} else if reply.Err == ErrWrongLeader || reply.Err == ErrFailed {
				ck.checkoutLeaderId(ck.leaderId + 1)
			} else {
				ck.log("reply error%+v", reply.Err)
				return
			}
		} else {
			ck.checkoutLeaderId(ck.leaderId + 1)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (ck *Clerk) Query(num int) Config {
	op := Op{Value: &QueryArgs{Num: num}, OpType: Query, ClientId: ck.clientID, SeqId: ck.seq}
	reply := Reply{}
	ck.doOp(&op, &reply)
	c := reply.Result.(Config)
	return c
}

func (ck *Clerk) Join(servers map[int][]string) {
	op := Op{Value: &JoinArgs{Servers: servers}, OpType: Join, ClientId: ck.clientID, SeqId: ck.seq}
	reply := Reply{}
	ck.doOp(&op, &reply)
}

func (ck *Clerk) Leave(gids []int) {
	op := Op{Value: &LeaveArgs{GIDs: gids}, OpType: Leave, ClientId: ck.clientID, SeqId: ck.seq}
	reply := Reply{}
	ck.doOp(&op, &reply)
}

func (ck *Clerk) Move(shard int, gid int) {
	op := Op{Value: &MoveArgs{Shard: shard, GID: gid}, OpType: Move, ClientId: ck.clientID, SeqId: ck.seq}
	reply := Reply{}
	ck.doOp(&op, &reply)
}
