package shardkv

import (
	"bytes"
	"context"
	"fmt"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardctrler"
	"6.5840/util"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Op string //get, put, append, updateConfig

	//正常的kv操作
	Key      string
	Value    string
	Sequence UniqSeq

	//配置更新
	NewConfig shardctrler.Config

	//分片搬迁操作
	//MoveShard map[int]bool

	//用于接收snapshot
	KVShards map[int]KVShard

	//用于接收ops
	CacheOps      []KVOp
	IsOver        bool
	ShardsVersion map[int]int

	//用于控制搬迁的进度
	//CachePos    int
	SendGroupId int
}
type KVOp struct {
	Op       string
	Key      string
	Value    string
	Sequence UniqSeq
}

type OpResult struct {
	Err   Err
	Value string
	Seq   UniqSeq
}

type SequenceAndRespChannel struct {
	sequence    UniqSeq
	respChannel chan OpResult
}

type KVShard struct {
	MemTable map[string]string
	// TODO 增加一个两天未使用，异步清理的流程
	FilterTable map[int64]OpResult
}

// TODO 是否可以将内存分类，当内存吃紧的时候，先回收低优先级内存
type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	cancelFunc    context.CancelFunc
	cancelContext context.Context

	//以下三个部分都要做snapshot
	kvShards map[int]KVShard

	//当分片搬迁时，用于保证幂等
	kvShardsVersion map[int]int

	//如果请求到来，分片在waitReceiveShard中，则令用户请求等待
	waitReceiveShard map[int]KVShard

	waitReceiveShardState map[int]ReceiveShardsState

	//记录正在搬迁的分片
	cacheOps []KVOp

	lastIndex    int
	index2result map[int]SequenceAndRespChannel

	//3B
	persister         *raft.Persister
	notifyTrySnapshot chan bool

	//4B
	shardCtrlClerk *shardctrler.Clerk
	config         shardctrler.Config

	group2WaitMoveShard map[int]map[int]bool
	movingShards        map[int]KVShard
	movingGroup         int
	movingServers       []string
	movingShardsState   moveShardsState
	gid2leader          map[int]int
	noMoreCacheOpsBegin int

	configUpdateChannel chan int
}

func (kv *ShardKV) CommonHandle(pOp Op) (rOpResult OpResult) {
	kv.mu.Lock()
	shardId := key2shard(pOp.Key)
	shard, ok := kv.kvShards[shardId]
	//fmt.Printf("group[%v] kv[%v] shards[%v] op[%v] shardid[%v]\n", kv.gid, kv.me, kv.kvShards, pOp, shardId)
	if !ok {
		//如果不存在，且当前不是
		if kv.config.Shards[shardId] != kv.gid {
			//fmt.Printf("group[%v] kv[%v] config[%v]\n", kv.gid, kv.me, kv.config)
			rOpResult = OpResult{Err: ErrWrongGroup}
			kv.mu.Unlock()
			return
		}
		//这里让客户端过段时间再重试
		rOpResult = OpResult{Err: ErrTryAgainLater}
		kv.mu.Unlock()
		return
	}
	if _, ok := kv.movingShards[shardId]; ok && kv.movingShardsState == &gMoveShardsNoCacheMoreOpsState {
		rOpResult = OpResult{Err: ErrWrongGroup}
		kv.mu.Unlock()
		return
	}
	if result, ok := shard.FilterTable[pOp.Sequence.ClientId]; ok && result.Seq == pOp.Sequence {
		rOpResult = result
		//fmt.Printf("op[%v] result[%v]\n", pOp, rOpResult)
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	rOpResult = kv.raftWait(pOp)
	return
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{Op: "Get", Key: args.Key, Sequence: args.Sequence}
	rOpResult := kv.CommonHandle(op)
	*reply = GetReply{Err: rOpResult.Err, Value: rOpResult.Value}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{Op: args.Op, Key: args.Key, Value: args.Value, Sequence: args.Sequence}
	rOpResult := kv.CommonHandle(op)
	*reply = PutAppendReply{Err: rOpResult.Err}

}
func (kv *ShardKV) raftWait(pOp Op) (rOpResult OpResult) {
	index, _, isLeader := kv.rf.Start(pOp)
	if !isLeader {
		//Debug(DebugClient, "group[%v] raft[%v] op[%v] not leader", kv.gid, kv.me, pOp)
		rOpResult = OpResult{Err: ErrWrongLeader}
		return
	}
	//util.Debug(util.DebugClient, kv.me,"group[%v] raft[%v] start[%v] op[%+v]", kv.gid, kv.me, index, pOp)
	respChannel := make(chan OpResult)
	kv.mu.Lock()
	if lastChannel, ok := kv.index2result[index]; ok {
		lastChannel.respChannel <- OpResult{Err: ErrWrongLeader}
	}
	kv.index2result[index] = SequenceAndRespChannel{sequence: pOp.Sequence, respChannel: respChannel}
	kv.mu.Unlock()
	timeoutTimer := time.NewTimer(100 * time.Millisecond)
	select {
	case opResult := <-respChannel:
		rOpResult = opResult
	case <-timeoutTimer.C:
		kv.mu.Lock()
		if resp, ok := kv.index2result[index]; ok && resp.sequence == pOp.Sequence {
			delete(kv.index2result, index)
		}
		kv.mu.Unlock()
		rOpResult = OpResult{Err: ErrTimeOut}
	}
	return
}

//这里operator是顺序执行，如果可能并发，则不能复用一个operator对象

//如何判断所有的日志都已经提交？？
func (kv *ShardKV) applier() {
	for {
		select {
		case <-kv.cancelContext.Done():
			return
		case applyMsg := <-kv.applyCh:
			if applyMsg.CommandValid {
				//operatorFactory
				op := applyMsg.Command.(Op)
				//util.Debug(util.DebugKV, "group[%v] kv[%v] op[%v] key[%v] value[%v] in applier out of applier", kv.gid, kv.me, op.Op, op.Key, op.Value)
				if op.Op != "Get" && op.Op != "Put" && op.Op != "Append" {
					util.Debug(util.DebugServerRaft, kv.me, "group[%v] raft[%v] index[%v] op[%+v]", kv.gid, kv.me, applyMsg.CommandIndex, applyMsg)
				}
				operator := operatorFactory(op.Op)
				if operator == nil {
					fmt.Printf("Fatal: Wrong Op=[%v]", applyMsg)
					break
				}
				kv.mu.Lock()
				result := operator.raftOps(kv, applyMsg)
				kv.lastIndex = applyMsg.CommandIndex
				resp, ok := kv.index2result[applyMsg.CommandIndex]
				if ok {
					delete(kv.index2result, applyMsg.CommandIndex)
				}
				kv.mu.Unlock()
				if ok {
					if resp.sequence == op.Sequence {
						resp.respChannel <- result
					} else {
						resp.respChannel <- OpResult{Err: "Leader Has Change"}
					}
				}
			} else if applyMsg.SnapshotValid {
				readBuffer := bytes.NewBuffer(applyMsg.Snapshot)
				decoder := labgob.NewDecoder(readBuffer)
				decodeKVShards := make(map[int]KVShard, 0)
				var decodeConfig shardctrler.Config
				decodeWaitReceiveShards := make(map[int]KVShard, 0)
				decodeGroup2WaitMoveShard := make(map[int]map[int]bool, 0)
				decodeMovingShards := make(map[int]KVShard, 0)
				var decodeMovingGroup int
				decodeMovingServers := make([]string, 0)
				var decodeMoveShardsState moveShardsState
				var decodeNoMoreCacheOpsBegin int
				var decodeCacheOps []KVOp
				var decodeWaitReceiveShardsState map[int]ReceiveShardsState
				var decodeKvshardsVersion map[int]int

				err := decoder.Decode(&decodeKVShards)
				if err != nil {
					fmt.Printf("Fatal:decoder.Decode(&decodeKVShards)=%v", err)
					return
				}
				err = decoder.Decode(&decodeConfig)
				if err != nil {
					fmt.Printf("Fatal:decoder.Decode(&decodeConfig)=%v", err)
					return
				}

				err = decoder.Decode(&decodeWaitReceiveShards)
				if err != nil {
					fmt.Printf("Fatal:decoder.Decode(&decodeConfig)=%v", err)
					return
				}

				err = decoder.Decode(&decodeGroup2WaitMoveShard)
				if err != nil {
					fmt.Printf("Fatal:decoder.Decode(&decodeConfig)=%v", err)
					return
				}

				err = decoder.Decode(&decodeMovingShards)
				if err != nil {
					fmt.Printf("Fatal:decoder.Decode(&decodeConfig)=%v", err)
					return
				}

				err = decoder.Decode(&decodeMovingGroup)
				if err != nil {
					fmt.Printf("Fatal:decoder.Decode(&decodeConfig)=%v", err)
					return
				}

				err = decoder.Decode(&decodeMovingServers)
				if err != nil {
					fmt.Printf("Fatal:decoder.Decode(&decodeConfig)=%v", err)
					return
				}

				var decodeMoveStateInt int
				err = decoder.Decode(&decodeMoveStateInt)
				if err != nil {
					fmt.Printf("Fatal:decoder.Decode(&decodeConfig)=%v", err)
					return
				}
				decodeMoveShardsState = int2state(decodeMoveStateInt)
				err = decoder.Decode(&decodeNoMoreCacheOpsBegin)
				if err != nil {
					fmt.Printf("Fatal:decoder.Decode(&decodeConfig)=%v", err)
					return
				}
				err = decoder.Decode(&decodeCacheOps)
				if err != nil {
					fmt.Printf("Fatal:decoder.Decode(&decodeConfig)=%v", err)
					return
				}
				err = decoder.Decode(&decodeWaitReceiveShardsState)
				if err != nil {
					fmt.Printf("Fatal:decoder.Decode(&decodeWaitReceiveShardsState)=%v", err)
					return
				}

				err = decoder.Decode(&decodeKvshardsVersion)
				if err != nil {
					fmt.Printf("Fatal:decoder.Decode(&decodeKvshardsVersion)=%v", err)
					return
				}

				kv.mu.Lock()
				kv.kvShards = decodeKVShards
				if applyMsg.SnapshotIndex > kv.lastIndex {
					kv.lastIndex = applyMsg.SnapshotIndex
				}

				kv.config = decodeConfig
				kv.waitReceiveShard = decodeWaitReceiveShards
				kv.group2WaitMoveShard = decodeGroup2WaitMoveShard
				kv.movingShards = decodeMovingShards
				kv.movingGroup = decodeMovingGroup
				kv.movingServers = decodeMovingServers
				kv.movingShardsState = decodeMoveShardsState
				kv.noMoreCacheOpsBegin = decodeNoMoreCacheOpsBegin
				kv.cacheOps = decodeCacheOps
				kv.waitReceiveShardState = decodeWaitReceiveShardsState

				kv.kvShardsVersion = decodeKvshardsVersion

				kv.mu.Unlock()
				util.Debug(util.DebugKVSnapshot, kv.me, "group[%v] raft[%v] shards[%v]", kv.gid, kv.me, decodeKVShards)
			}
			kv.notifyTrySnapshot <- true
		}
	}
}

func (kv *ShardKV) snapshotTicker() {
	for {
		select {
		case <-kv.cancelContext.Done():
			return
		case <-kv.notifyTrySnapshot:
			if kv.maxraftstate <= 0 || kv.persister.RaftStateSize() < kv.maxraftstate {
				break
			}
			kv.mu.Lock()
			//TODO 如何做到写时复制？
			//TODO 对Config做Snapshot
			copyKVShards := make(map[int]KVShard, len(kv.kvShards))
			for key, oneShard := range kv.kvShards {
				copyOneShard := KVShard{}
				copyOneShard.FilterTable = make(map[int64]OpResult)
				copyOneShard.MemTable = make(map[string]string)
				for key, value := range oneShard.MemTable {
					copyOneShard.MemTable[key] = value
				}
				for key, value := range oneShard.FilterTable {
					copyOneShard.FilterTable[key] = value
				}
				copyKVShards[key] = copyOneShard
			}

			copyConfig := kv.config
			copyConfig.Groups = make(map[int][]string)
			for key, value := range kv.config.Groups {
				copyConfig.Groups[key] = make([]string, len(value))
				copy(copyConfig.Groups[key], value)
			}

			//拷贝waitReceiveShards
			copyWaitReceiveShards := make(map[int]KVShard, len(kv.waitReceiveShard))
			for key, oneShard := range kv.waitReceiveShard {
				copyOneShard := KVShard{}
				copyOneShard.FilterTable = make(map[int64]OpResult)
				copyOneShard.MemTable = make(map[string]string)
				for key, value := range oneShard.MemTable {
					copyOneShard.MemTable[key] = value
				}
				for key, value := range oneShard.FilterTable {
					copyOneShard.FilterTable[key] = value
				}
				copyWaitReceiveShards[key] = copyOneShard
			}

			copyGroup2WaitMoveShard := make(map[int]map[int]bool, len(kv.group2WaitMoveShard))
			for goupId, moveShards := range kv.group2WaitMoveShard {
				oneMoveShard := make(map[int]bool, len(moveShards))
				for oneShard, _ := range moveShards {
					oneMoveShard[oneShard] = true
				}
				copyGroup2WaitMoveShard[goupId] = oneMoveShard
			}
			copyMovingShards := make(map[int]KVShard, len(kv.movingShards))
			for key, oneShard := range kv.movingShards {
				copyOneShard := KVShard{}
				copyOneShard.FilterTable = make(map[int64]OpResult)
				copyOneShard.MemTable = make(map[string]string)
				for key, value := range oneShard.MemTable {
					copyOneShard.MemTable[key] = value
				}
				for key, value := range oneShard.FilterTable {
					copyOneShard.FilterTable[key] = value
				}
				copyMovingShards[key] = copyOneShard
			}
			copyMovingGroup := kv.movingGroup
			copyMovingServers := make([]string, len(kv.movingServers))
			for key, value := range kv.movingServers {
				copyMovingServers[key] = value
			}
			copyMovingShardsState := kv.movingShardsState
			copyNoMoreCacheOpsBegin := kv.noMoreCacheOpsBegin
			copyCacheOps := make([]KVOp, len(kv.cacheOps))
			for key, value := range kv.cacheOps {
				copyCacheOps[key] = value
			}
			copyWaitReceiveShardsState := make(map[int]ReceiveShardsState, len(kv.waitReceiveShardState))
			for key, value := range kv.waitReceiveShardState {
				copyWaitReceiveShardsState[key] = value
			}

			copyKvShardsVersion := make(map[int]int, len(kv.kvShardsVersion))
			for key, value := range kv.kvShardsVersion {
				copyKvShardsVersion[key] = value
			}

			lastIndex := kv.lastIndex
			kv.mu.Unlock()
			byteBuffer := new(bytes.Buffer)
			encoder := labgob.NewEncoder(byteBuffer)
			err := encoder.Encode(copyKVShards)
			if err != nil {
				fmt.Printf("Fatal:KVShards encoder.Encode(%v)=%v\n", copyKVShards, err)
				break
			}
			err = encoder.Encode(copyConfig)
			if err != nil {
				fmt.Printf("Fatal:KVShards encoder.Encode(%v)=%v\n", copyConfig, err)
				break
			}
			err = encoder.Encode(copyWaitReceiveShards)
			if err != nil {
				fmt.Printf("Fatal:KVShards encoder.Encode(%v)=%v\n", copyWaitReceiveShards, err)
				break
			}
			err = encoder.Encode(copyGroup2WaitMoveShard)
			if err != nil {
				fmt.Printf("Fatal:KVShards encoder.Encode(%v)=%v\n", copyGroup2WaitMoveShard, err)
				break
			}

			err = encoder.Encode(copyMovingShards)
			if err != nil {
				fmt.Printf("Fatal:KVShards encoder.Encode(%v)=%v\n", copyMovingShards, err)
				break
			}
			err = encoder.Encode(copyMovingGroup)
			if err != nil {
				fmt.Printf("Fatal:KVShards encoder.Encode(%v)=%v\n", copyMovingGroup, err)
				break
			}
			err = encoder.Encode(copyMovingServers)
			if err != nil {
				fmt.Printf("Fatal:KVShards encoder.Encode(%v)=%v\n", copyMovingServers, err)
				break
			}
			movingShardsStateInt := state2int(copyMovingShardsState)
			err = encoder.Encode(movingShardsStateInt)
			if err != nil {
				fmt.Printf("Fatal:KVShards encoder.Encode(%v)=%v\n", copyMovingShardsState, err)
				break
			}
			err = encoder.Encode(copyNoMoreCacheOpsBegin)
			if err != nil {
				fmt.Printf("Fatal:KVShards encoder.Encode(%v)=%v\n", copyNoMoreCacheOpsBegin, err)
				break
			}
			err = encoder.Encode(copyCacheOps)
			if err != nil {
				fmt.Printf("Fatal:KVShards encoder.Encode(%v)=%v\n", copyCacheOps, err)
				break
			}
			err = encoder.Encode(copyWaitReceiveShardsState)
			if err != nil {
				fmt.Printf("Fatal:KVShards encoder.Encode(%v)=%v\n", copyWaitReceiveShardsState, err)
				break
			}

			err = encoder.Encode(copyKvShardsVersion)
			if err != nil {
				fmt.Printf("Fatal:KVShards encoder.Encode(%v)=%v\n", copyWaitReceiveShardsState, err)
				break
			}
			kv.rf.Snapshot(lastIndex, byteBuffer.Bytes())
			//fmt.Printf("kv[%v] end snapshot raft state size[%v]\n", kv.me, kv.persister.RaftStateSize())
		}

	}
}

//第一个问题，需不需要由leader管理config
//如果不由leader统一管理config，则需要处理以下两种情况
//1.follower的config比leader落后
//2.follower的config比leader先进
//第二个问题，如果其他的set的配置比当前set的配置新
//这里解决方法是，无条件接收分片

func (kv *ShardKV) configUpdateTicker() {
	configUpdateTicker := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-kv.cancelContext.Done():
			configUpdateTicker.Stop()
			return
		case <-kv.configUpdateChannel:
			//?这边有没有更好的实现方式?
			kv.configUpdate()
		case <-configUpdateTicker.C:
			//连续更新
			kv.configUpdate()
		}
	}
}
func (kv *ShardKV) configUpdate() {
	for {
		kv.mu.Lock()
		nowConfig := kv.config
		kv.mu.Unlock()
		newConfig := kv.shardCtrlClerk.Query(nowConfig.Num + 1)
		if newConfig.Num != nowConfig.Num+1 {
			break
		}
		//fmt.Printf("try update config now[%v] new[%v]\n", nowConfig, newConfig)
		//利用raft过程，更新config
		op := Op{Op: "configUpdate", NewConfig: newConfig}
		result := kv.raftWait(op)
		if result.Err != OK {
			break
		}
	}
}

//需要标识哪些分片需要搬迁
//这里比较好的方式，还是时间驱动，如果是事件驱动，需要同时感知两个事件，一个是config变化，一个是raft的state变化，还有分片的变化
//一个简单的方式，是时间驱动
//传递序列化以后的分片
//因为整个流程中，涉及多个raft过程，所以有两种方式连接多个阶段
//1.通过中间变量，连接多个阶段，每个阶段有一个协程，检查对应的变量
//2.有去有回的raft过程，连接多个阶段，这种方式有一个问题，如何发现多个阶段
//3.关于使用状态模式，这里有两点比较特别
//	1.调用状态模式的处理函数时，会加锁，但是不会解锁，由于状态模式有一个raft过程，所以自己做解锁
//  2.在raft过程中做状态更新
func (kv *ShardKV) moveShardsTicker() {
	tryBeginMoveShardTicker := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-kv.cancelContext.Done():
			return
		case <-tryBeginMoveShardTicker.C:
			for {
				_, isLeader := kv.rf.GetState()
				if !isLeader {
					break
				}
				//这里没有解锁，算是跟movingShardsState的一个契约，在调用handle之前，已经加锁handle函数自己控制解锁
				kv.mu.Lock()
				result := kv.movingShardsState.handle(kv)
				if result == ErrTryAgainLater {
					break
				}
			}

		}
	}
}

//TODO 接收到一个新的分片以后，要判断一下，是否需要搬迁
func (kv *ShardKV) ReceiveShardsSnapshot(args *ReceiveShardsSnapshotArgs, reply *ReceiveShardsSnapshotReply) {
	kv.mu.Lock()
	if args.ConfigNum > kv.config.Num {
		kv.mu.Unlock()
		kv.configUpdateChannel <- args.ConfigNum
		*reply = ReceiveShardsSnapshotReply{Err: ErrTryAgainLater}
		return
	}
	//TODO 是否比对一个便可
	for key, value := range args.ShardsVersion {
		if value <= kv.kvShardsVersion[key] {
			delete(args.KVShards, key)
			delete(args.ShardsVersion, key)
		}
	}
	kv.mu.Unlock()
	if len(args.KVShards) == 0 {
		*reply = ReceiveShardsSnapshotReply{Err: OK}
		return
	}
	//反序列化snapshot
	op := Op{Op: "ReceiveShardsSnapshot", KVShards: args.KVShards, SendGroupId: args.SendGroupId, ShardsVersion: args.ShardsVersion}
	tResult := kv.raftWait(op)
	if tResult.Err != OK {
		util.Debug(util.DebugMoveShardState, kv.me, "group[%v] raft[%v] ReceiveSnapshot[%v] Err[%v]", kv.gid, kv.me, op, tResult)
	}
	*reply = ReceiveShardsSnapshotReply{Err: tResult.Err}
}

func (kv *ShardKV) ReceiveShardsOps(args *ReceiveShardsOpsArgs, reply *ReceiveShardsOpsReply) {
	/*这里不要做过滤，raft过程可能还没有结束，kv.shardVersion可能还没有更新，可能会导致错误
	kv.mu.Lock()
	shardsVersion := make(map[int]int, 0)
	for key, value := range args.ShardsVersion {
		if value == kv.kvShardsVersion[key] {
			shardsVersion[key] = value
		}
	}

	if len(shardsVersion) == 0 {
		util.Debug(util.DebugMoveShardState, kv.me, "group[%v] raft[%v] args[%v] localShardsVersion[%v]", kv.gid, kv.me, *args, kv.kvShardsVersion)
		*reply = ReceiveShardsOpsReply{Err: OK}
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()*/
	op := Op{Op: "ReceiveShardsOps", CacheOps: args.Ops, IsOver: args.IsOver, ShardsVersion: args.ShardsVersion, SendGroupId: args.SendGroupId}
	tResult := kv.raftWait(op)
	*reply = ReceiveShardsOpsReply{Err: tResult.Err}
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	/*for {
		kv.mu.Lock()
		if len(kv.kvShards) == 0 {
			kv.mu.Unlock()
			break
		}
		kv.mu.Unlock()
		time.Sleep(time.Millisecond * 100)
	}*/
	kv.rf.Kill()
	// Your code here, if desired.
	kv.cancelFunc()
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
// 注意，确保每个goroute能在服务关闭以后，自动关闭
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.persister = persister
	kv.notifyTrySnapshot = make(chan bool)
	kv.applyCh = make(chan raft.ApplyMsg)
	//kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvShards = make(map[int]KVShard)

	kv.waitReceiveShard = make(map[int]KVShard)
	kv.waitReceiveShardState = make(map[int]ReceiveShardsState)

	kv.group2WaitMoveShard = make(map[int]map[int]bool)
	kv.movingShards = make(map[int]KVShard)

	kv.index2result = make(map[int]SequenceAndRespChannel)
	kv.cancelContext, kv.cancelFunc = context.WithCancel(context.Background())

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	kv.shardCtrlClerk = shardctrler.MakeClerk(kv.ctrlers)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.gid2leader = make(map[int]int)
	kv.movingShardsState = &gNoMoveShardsState
	kv.configUpdateChannel = make(chan int, 10)
	kv.kvShardsVersion = make(map[int]int)
	//读取是否存在本地文件，如果不存在，则读取1号配置，创建对应的分片
	//可以提供一个注册函数，就不用每次添加了
	go kv.moveShardsTicker()
	go kv.applier()
	go kv.snapshotTicker()
	go kv.configUpdateTicker()
	//kv.configUpdateChannel <- 1
	return kv
}
