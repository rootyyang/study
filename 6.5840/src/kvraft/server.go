package kvraft

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"github.com/sasha-s/go-deadlock"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op       string
	Key      string
	Value    string
	Sequence UniqSeq
}
type SequenceAndOpResult struct {
	Seq int
	OpResult
}

type OpResult struct {
	Err   Err
	Value string
}
type SequenceAndRespChannel struct {
	sequence    UniqSeq
	respChannel chan OpResult
}

type KVServer struct {
	mu      deadlock.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	cancelFunc    context.CancelFunc
	cancelContext context.Context

	//以下三个部分都要做snapshot
	memTable map[string]string

	// TODO 增加一个最长最久未使用
	filterTable map[int64]SequenceAndOpResult

	lastIndex    int
	index2result map[int]SequenceAndRespChannel

	//3B
	persister         *raft.Persister
	notifyTrySnapshot chan bool
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if result, ok := kv.filterTable[args.Sequence.ClientId]; ok && result.Seq == args.Sequence.Seq {
		*reply = GetReply{Err: result.Err, Value: result.Value}
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op{Op: "Get", Key: args.Key, Sequence: args.Sequence}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		*reply = GetReply{Err: "node not leader"}
		return
	}
	//fmt.Printf("kv[%v] Get start[%v]\n", kv.me, index)
	respChannel := make(chan OpResult)
	kv.mu.Lock()
	if lastChannel, ok := kv.index2result[index]; ok {
		lastChannel.respChannel <- OpResult{Err: "node not leader anymore"}
	}
	kv.index2result[index] = SequenceAndRespChannel{sequence: args.Sequence, respChannel: respChannel}
	kv.mu.Unlock()
	timeoutTimer := time.NewTimer(100 * time.Millisecond)
	select {
	case opResult := <-respChannel:
		*reply = GetReply{Err: opResult.Err, Value: opResult.Value}
	case <-timeoutTimer.C:
		kv.mu.Lock()
		if resp, ok := kv.index2result[index]; ok && resp.sequence == args.Sequence {
			delete(kv.index2result, index)
		}
		kv.mu.Unlock()
		*reply = GetReply{Err: "time out"}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if result, ok := kv.filterTable[args.Sequence.ClientId]; ok && result.Seq == args.Sequence.Seq {
		*reply = PutAppendReply{Err: result.Err}
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	op := Op{Op: args.Op, Key: args.Key, Value: args.Value, Sequence: args.Sequence}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		*reply = PutAppendReply{Err: "node not leader"}
		return
	}
	//fmt.Printf("kv[%v] PutAppend  start[%v]\n", kv.me, index)
	respChannel := make(chan OpResult)
	kv.mu.Lock()
	if lastChannel, ok := kv.index2result[index]; ok {
		lastChannel.respChannel <- OpResult{Err: "node not leader anymore"}
	}
	kv.index2result[index] = SequenceAndRespChannel{sequence: args.Sequence, respChannel: respChannel}
	kv.mu.Unlock()
	timeoutTimer := time.NewTimer(100 * time.Millisecond)
	select {
	case opResult := <-respChannel:
		*reply = PutAppendReply{Err: opResult.Err}
	case <-timeoutTimer.C:
		kv.mu.Lock()
		if resp, ok := kv.index2result[index]; ok && resp.sequence == args.Sequence {
			delete(kv.index2result, index)
		}
		kv.mu.Unlock()
		*reply = PutAppendReply{Err: "time out"}
	}
}

//幂等队列，应该在应用的同时，添加，如果应用之前发现已经有了，则不再应用
func (kv *KVServer) applier() {
	for {
		select {
		case <-kv.cancelContext.Done():
			return
		case applyMsg := <-kv.applyCh:
			DPrintf("kv[%v] in Applier [%+v]\n", kv.me, applyMsg)
			if applyMsg.CommandValid {
				op := applyMsg.Command.(Op)
				//fmt.Printf("kv[%v] Applier [%v]\n", kv.me, applyMsg.CommandIndex)
				kv.mu.Lock()
				//清理过期的filtertable
				var result SequenceAndOpResult
				var ok bool

				kv.lastIndex = applyMsg.CommandIndex
				if result, ok = kv.filterTable[op.Sequence.ClientId]; !ok || result.Seq != op.Sequence.Seq {
					switch op.Op {
					case "Get":
						result.Value = kv.memTable[op.Key]
					case "Put":
						DPrintf("kv[%v] in applier Put op[%+v]\n", kv.me, op)
						kv.memTable[op.Key] = op.Value
					case "Append":
						DPrintf("kv[%v] in applier Append op[%+v]\n", kv.me, op)
						value := kv.memTable[op.Key]
						kv.memTable[op.Key] = value + op.Value
					}
					result.Seq = op.Sequence.Seq
					kv.filterTable[op.Sequence.ClientId] = result
				}
				resp, ok := kv.index2result[applyMsg.CommandIndex]
				if ok {
					delete(kv.index2result, applyMsg.CommandIndex)
				}
				kv.mu.Unlock()
				if ok {
					if resp.sequence == op.Sequence {
						resp.respChannel <- result.OpResult
					} else {
						resp.respChannel <- OpResult{Err: "Leader Has Change"}
					}
				}
			} else if applyMsg.SnapshotValid {
				readBuffer := bytes.NewBuffer(applyMsg.Snapshot)
				decoder := labgob.NewDecoder(readBuffer)
				memtable := make(map[string]string)
				filtertable := make(map[int64]SequenceAndOpResult)
				err := decoder.Decode(&memtable)
				if err != nil {
					fmt.Printf("Fatal:decoder.Decode(&memtable)=%v", err)
					return
				}
				err = decoder.Decode(&filtertable)
				if err != nil {
					fmt.Printf("Fatal:decoder.Decode(&filtertable)=%v", err)
					return
				}
				kv.mu.Lock()
				kv.memTable = memtable
				kv.filterTable = filtertable
				if applyMsg.SnapshotIndex > kv.lastIndex {
					kv.lastIndex = applyMsg.SnapshotIndex
				}
				kv.mu.Unlock()
			}
			kv.notifyTrySnapshot <- true
		}
	}
}

func (kv *KVServer) snapshotRoute() {
	for {
		select {
		case <-kv.cancelContext.Done():
			return
		case <-kv.notifyTrySnapshot:
			if kv.maxraftstate <= 0 || kv.persister.RaftStateSize() < kv.maxraftstate {
				break
			}
			kv.mu.Lock()
			//TODO如何做到写时复制？
			copyMemTable := make(map[string]string, len(kv.memTable))
			copyFilterTable := make(map[int64]SequenceAndOpResult, len(kv.filterTable))
			for key, value := range kv.memTable {
				copyMemTable[key] = value
			}
			for key, value := range kv.filterTable {
				copyFilterTable[key] = value
			}
			lastIndex := kv.lastIndex
			kv.mu.Unlock()

			byteBuffer := new(bytes.Buffer)
			encoder := labgob.NewEncoder(byteBuffer)
			err := encoder.Encode(copyMemTable)
			if err != nil {
				fmt.Printf("Fatal:memtable encoder.Encode(%v)=%v\n", copyMemTable, err)
				break
			}
			err = encoder.Encode(copyFilterTable)
			if err != nil {
				fmt.Printf("Fatal:filterTable encoder.Encode(%v)=%v\n", copyFilterTable, err)
				break
			}
			kv.rf.Snapshot(lastIndex, byteBuffer.Bytes())
			//fmt.Printf("kv[%v] end snapshot raft state size[%v]\n", kv.me, kv.persister.RaftStateSize())
		}

	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	kv.cancelFunc()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.persister = persister
	kv.notifyTrySnapshot = make(chan bool)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.memTable = make(map[string]string)
	kv.filterTable = make(map[int64]SequenceAndOpResult)
	kv.index2result = make(map[int]SequenceAndRespChannel)
	kv.cancelContext, kv.cancelFunc = context.WithCancel(context.Background())
	go kv.applier()
	go kv.snapshotRoute()
	return kv
}
