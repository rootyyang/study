package shardctrler

//
// Shardctrler clerk.
//

import (
	"math/rand"
	"sync"
	"time"

	"6.5840/labrpc"
	"6.5840/util"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	mu         sync.Mutex
	lastLeader int
	clientId   int64
	seq        int
}

/*
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}*/

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = time.Now().UnixNano()
	ck.lastLeader = rand.Int() % len(servers)
	return ck
}

func (ck *Clerk) Query(num int) Config {
	// Your code here.
	ck.mu.Lock()
	tmpLeader := ck.lastLeader
	args := QueryArgs{Num: num, Sequence: UniqSeq{ClientId: ck.clientId, Seq: ck.seq}}
	ck.seq++
	ck.mu.Unlock()
	//如果三个节点都调用rpc错误，则报错
	failNum := 0
	for {
		var reply QueryReply
		ok := ck.servers[tmpLeader].Call("ShardCtrler.Query", &args, &reply)
		util.Debug(util.DebugShardctl, 0, "query args[%v] reply[%v] ok[%v] leader[%v] servers[%v]", args, reply, ok, tmpLeader, ck.servers)
		if ok {
			failNum = 0
			if !reply.WrongLeader {
				return reply.Config
			}
		} else {
			failNum++
			if failNum == 3 {
				return Config{}
			}
		}
		// try each known server.
		ck.mu.Lock()
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		tmpLeader = ck.lastLeader
		ck.mu.Unlock()
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.mu.Lock()
	tmpLeader := ck.lastLeader
	args := JoinArgs{Servers: servers, Sequence: UniqSeq{ClientId: ck.clientId, Seq: ck.seq}}
	ck.seq++
	ck.mu.Unlock()
	for {
		// try each known server.
		var reply JoinReply
		ok := ck.servers[tmpLeader].Call("ShardCtrler.Join", &args, &reply)
		if ok && !reply.WrongLeader {
			return
		}
		ck.mu.Lock()
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		tmpLeader = ck.lastLeader
		ck.mu.Unlock()
	}
}

func (ck *Clerk) Leave(gids []int) {

	ck.mu.Lock()
	tmpLeader := ck.lastLeader
	args := LeaveArgs{GIDs: gids, Sequence: UniqSeq{ClientId: ck.clientId, Seq: ck.seq}}
	ck.seq++
	ck.mu.Unlock()

	for {
		// try each known server.
		var reply LeaveReply
		ok := ck.servers[tmpLeader].Call("ShardCtrler.Leave", &args, &reply)
		if ok && !reply.WrongLeader {
			return
		}
		ck.mu.Lock()
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		tmpLeader = ck.lastLeader
		ck.mu.Unlock()
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.mu.Lock()
	tmpLeader := ck.lastLeader
	args := MoveArgs{Shard: shard, GID: gid, Sequence: UniqSeq{ClientId: ck.clientId, Seq: ck.seq}}
	ck.seq++
	ck.mu.Unlock()

	for {
		// try each known server.
		var reply MoveReply
		ok := ck.servers[tmpLeader].Call("ShardCtrler.Move", &args, &reply)
		if ok && !reply.WrongLeader {
			return
		}
		ck.mu.Lock()
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		tmpLeader = ck.lastLeader
		ck.mu.Unlock()
	}
}

/*
func (ck *Clerk) AllocateShard(shard []int, gid int) []int {
	ck.mu.Lock()
	tmpLeader := ck.lastLeader
	args := AllocateShardArgs{Shards: shard, GID: gid, Sequence: UniqSeq{ClientId: ck.clientId, Seq: ck.seq}}
	ck.seq++
	ck.mu.Unlock()
	for {
		// try each known server.
		var reply AllocateShardReply
		ok := ck.servers[tmpLeader].Call("ShardCtrler. AllocateShard", &args, &reply)
		if ok && !reply.WrongLeader {
			return reply.SuccessShards
		}
		ck.mu.Lock()
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		tmpLeader = ck.lastLeader
		ck.mu.Unlock()
	}
}*/
