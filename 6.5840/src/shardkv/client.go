package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
	"6.5840/util"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
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
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	mu sync.Mutex
	//lastLeader int
	clientId int64
	seq      int
	//TODO 增加一个根据config，异步清理
	gid2leader map[int]int
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	ck.gid2leader = make(map[int]int)
	// You'll have to add code here.
	ck.config = ck.sm.Query(-1)

	ck.clientId = time.Now().UnixNano()
	//ck.lastLeader = rand.Int() % len(servers)

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	//tmpLeader := ck.lastLeader
	args := GetArgs{Key: key, Sequence: UniqSeq{ClientId: ck.clientId, Seq: ck.seq}}
	ck.seq++
	ck.mu.Unlock()
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			ck.mu.Lock()
			leader := ck.gid2leader[gid]
			ck.mu.Unlock()
			faileTime := 0
			for {
				srv := ck.make_end(servers[leader])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				util.Debug(util.DebugClient, 0, "Get Args[%v] shardid[%v] Reply[%v] ok[%v] config[%v] servers[%v] leader[%v]", args, shard, reply, ok, ck.config, servers, leader)
				if ok {
					faileTime = 0
					if reply.Err == OK || reply.Err == ErrNoKey {
						return reply.Value
					}
					if reply.Err == ErrWrongGroup {
						break
					}
					if reply.Err == ErrTryAgainLater {
						time.Sleep(100 * time.Millisecond)
						continue
					}
				} else {
					faileTime++
					if faileTime == len(servers) {
						break
					}
				}
				leader = (leader + 1) % len(servers)
				ck.mu.Lock()
				ck.gid2leader[gid] = leader
				ck.mu.Unlock()
			}
			// try each server for the shard.
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
	return ""
}

// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.mu.Lock()
	//tmpLeader := ck.lastLeader
	args := PutAppendArgs{Key: key, Value: value, Op: op, Sequence: UniqSeq{ClientId: ck.clientId, Seq: ck.seq}}
	ck.seq++
	ck.mu.Unlock()
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			ck.mu.Lock()
			leader := ck.gid2leader[gid]
			ck.mu.Unlock()
			faileTime := 0
			for {
				srv := ck.make_end(servers[leader])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				util.Debug(util.DebugClient, 0, "%v Args[%v] shardid[%v] Reply[%v] ok[%v] servers[%v]", op, args, shard, reply, ok, servers)
				if ok {
					faileTime = 0
					if reply.Err == OK {
						return
					}
					if reply.Err == ErrWrongGroup {
						break
					}
					if reply.Err == ErrTryAgainLater {
						time.Sleep(100 * time.Millisecond)
						continue
					}
				} else {
					faileTime++
					if faileTime == len(servers) {
						break
					}
				}
				leader = (leader + 1) % len(servers)
				ck.mu.Lock()
				ck.gid2leader[gid] = leader
				ck.mu.Unlock()
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask controler for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
