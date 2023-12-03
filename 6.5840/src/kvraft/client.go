package kvraft

import (
	"math/rand"
	"sync"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu sync.Mutex

	lastLeader int
	//client id
	clientId int64
	seq      int
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
	// You'll have to add code here.
	// 如果要把保证全局唯一，可以再加IP，拼成一个字符串
	ck.clientId = time.Now().UnixNano()
	ck.lastLeader = rand.Int() % len(servers)
	return ck
}

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
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	tmpLeader := ck.lastLeader
	args := GetArgs{Key: key, Sequence: UniqSeq{ClientId: ck.clientId, Seq: ck.seq}}
	ck.seq++
	ck.mu.Unlock()
	for {
		reply := GetReply{}
		ok := ck.servers[tmpLeader].Call("KVServer.Get", &args, &reply)
		if ok && len(reply.Err) == 0 {
			return reply.Value
		}
		ck.mu.Lock()
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		tmpLeader = ck.lastLeader
		ck.mu.Unlock()
	}
	// You will have to modify this function.
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.mu.Lock()
	tmpLeader := ck.lastLeader
	args := PutAppendArgs{Key: key, Value: value, Op: op, Sequence: UniqSeq{ClientId: ck.clientId, Seq: ck.seq}}
	ck.seq++
	ck.mu.Unlock()
	for {
		reply := PutAppendReply{}
		ok := ck.servers[tmpLeader].Call("KVServer.PutAppend", &args, &reply)
		if ok && len(reply.Err) == 0 {
			return
		}
		ck.mu.Lock()
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		tmpLeader = ck.lastLeader
		ck.mu.Unlock()
		//time.Sleep(500 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
