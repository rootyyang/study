package shardkv

import (
	"sort"

	"6.5840/raft"
	"6.5840/util"
)

func init() {
	registerRaftOperator("noMoveShardsState", &gNoMoveShardsState)
	registerRaftOperator("moveShardsSnapshotState", &gMoveShardsSnapshotState)
	registerRaftOperator("moveShardsReplayCacheOpsState", &gMoveShardsReplayCacheOpsState)
	registerRaftOperator("moveShardsNoCacheMoreOpsState", &gMoveShardsNoCacheMoreOpsState)
	registerRaftOperator("ReceiveShardsSnapshot", &receiveShardsSnapshotOperator{})
	registerRaftOperator("ReceiveShardsOps", &receiveShardsOpsOperator{})
}

type ReceiveShardsState int

const (
	ReceiveShardSnapshot ReceiveShardsState = iota
	ReceiveShardCacheOps
)

var gState2Int = map[moveShardsState]int{&gNoMoveShardsState: 0, &gMoveShardsSnapshotState: 1, &gMoveShardsReplayCacheOpsState: 2, &gMoveShardsNoCacheMoreOpsState: 3}

var gInt2State = map[int]moveShardsState{
	0: &gNoMoveShardsState, 1: &gMoveShardsSnapshotState, 2: &gMoveShardsReplayCacheOpsState, 3: &gMoveShardsNoCacheMoreOpsState}

func state2int(pState moveShardsState) int {
	return gState2Int[pState]
}
func int2state(pInt int) moveShardsState {
	return gInt2State[pInt]
}

var gNoMoveShardsState noMoveShardsState = noMoveShardsState{}
var gMoveShardsSnapshotState moveShardsSnapshotState = moveShardsSnapshotState{}
var gMoveShardsReplayCacheOpsState moveShardsReplayCacheOpsState = moveShardsReplayCacheOpsState{}
var gMoveShardsNoCacheMoreOpsState moveShardsNoCacheMoreOpsState = moveShardsNoCacheMoreOpsState{}

//? 这么实现，是否更不好理解

//执行handle函数之前，默认加锁
type moveShardsState interface {
	handle(pkv *ShardKV) Err
}

type noMoveShardsState struct {
}

func (state *noMoveShardsState) handle(pkv *ShardKV) Err {
	if len(pkv.group2WaitMoveShard) == 0 {
		pkv.mu.Unlock()
		return ErrTryAgainLater
	}
	//util.Debug(util.DebugMoveShardState, "group[%v] raft[%v] try begin move", pkv.gid, pkv.me)

	pkv.mu.Unlock()
	op := Op{Op: "noMoveShardsState"}
	result := pkv.raftWait(op)
	if result.Err != OK {
		util.Debug(util.DebugMoveShardState, pkv.me, "group[%v] raft[%v] try begin move Err[%v]", pkv.gid, pkv.me, result)
		return ErrTryAgainLater
	}
	return OK
}
func (ms *noMoveShardsState) raftOps(pkv *ShardKV, pApplyMsg raft.ApplyMsg) OpResult {
	if pkv.movingShardsState != ms || len(pkv.group2WaitMoveShard) == 0 {
		return OpResult{Err: OK}
	}
	allGroups := make([]int, 0, len(pkv.group2WaitMoveShard))
	for key, _ := range pkv.group2WaitMoveShard {
		allGroups = append(allGroups, key)
	}
	sort.Ints(allGroups)
	pkv.movingGroup = allGroups[0]
	pkv.movingServers = pkv.config.Groups[pkv.movingGroup]
	copyKVShards := make(map[int]KVShard, len(pkv.group2WaitMoveShard[pkv.movingGroup]))
	for key, _ := range pkv.group2WaitMoveShard[pkv.movingGroup] {
		oneShard, ok := pkv.kvShards[key]
		if !ok {
			continue
		}
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
		//更新版本
		pkv.kvShardsVersion[key]++
	}
	pkv.movingShards = copyKVShards
	//像一个方法，声名式的封装状态迁移
	pkv.movingShardsState = &gMoveShardsSnapshotState
	util.Debug(util.DebugServerRaft, pkv.me, "group[%v] raft[%v] in raft move[%v] to group[%v]", pkv.gid, pkv.me, pkv.movingShards, pkv.movingGroup)
	delete(pkv.group2WaitMoveShard, pkv.movingGroup)
	return OpResult{Err: OK}
}

type moveShardsSnapshotState struct {
}

func (state *moveShardsSnapshotState) handle(pkv *ShardKV) Err {
	copyKVShards := pkv.movingShards
	// TODO 优化一下，增加一个数据搬迁的流量控制
	receiveGid := pkv.movingGroup
	receiveServers := pkv.movingServers
	leader := pkv.gid2leader[receiveGid]
	shardsVersion := make(map[int]int, len(copyKVShards))
	for key, _ := range copyKVShards {
		shardsVersion[key] = pkv.kvShardsVersion[key]
	}
	args := ReceiveShardsSnapshotArgs{ConfigNum: pkv.config.Num, SendGroupId: pkv.gid, KVShards: copyKVShards, ShardsVersion: shardsVersion}
	pkv.mu.Unlock()
	util.Debug(util.DebugMoveShardState, pkv.me, "group[%v] raft[%v] try begin move snapshot[%v] to group[%v]", pkv.gid, pkv.me, copyKVShards, pkv.movingGroup)
	failTime := 0
	for {
		//序列化分片
		srv := pkv.make_end(receiveServers[leader])
		var reply ReceiveShardsSnapshotReply
		ok := srv.Call("ShardKV.ReceiveShardsSnapshot", &args, &reply)
		if ok {
			failTime = 0
			if reply.Err == OK {
				op := Op{Op: "moveShardsSnapshotState"}
				result := pkv.raftWait(op)
				if result.Err != OK {
					return ErrTryAgainLater
				}
				return OK
			} else if reply.Err == ErrTryAgainLater {
				return ErrTryAgainLater
			}
		} else {
			failTime++
			if failTime == 3 {
				return ErrTryAgainLater
			}
		}
		leader = (leader + 1) % len(receiveServers)
		pkv.mu.Lock()
		pkv.gid2leader[receiveGid] = leader
		pkv.mu.Unlock()
	}
}

func (ms *moveShardsSnapshotState) raftOps(pkv *ShardKV, pApplyMsg raft.ApplyMsg) OpResult {
	if pkv.movingShardsState != ms {
		util.Debug(util.DebugServerRaft, pkv.me, "group[%v] raft[%v] in raft move ops state[%v] error", pkv.gid, pkv.me, pkv.movingShardsState)
		return OpResult{Err: OK}
	}
	util.Debug(util.DebugServerRaft, pkv.me, "group[%v] raft[%v] in raft move ops", pkv.gid, pkv.me)
	pkv.movingShardsState = &gMoveShardsReplayCacheOpsState
	pkv.noMoreCacheOpsBegin = len(pkv.cacheOps)
	return OpResult{Err: OK}
}

type moveShardsReplayCacheOpsState struct {
}

func (state *moveShardsReplayCacheOpsState) handle(pkv *ShardKV) Err {
	receiveGid := pkv.movingGroup
	receiveServers := pkv.movingServers
	leader := pkv.gid2leader[receiveGid]

	//状态轮转时的noMoreCacheOpsBegin必须保证幂等
	cacheOpsEnd := pkv.noMoreCacheOpsBegin
	shardsVersion := make(map[int]int, len(pkv.movingShards))
	for key, _ := range pkv.movingShards {
		shardsVersion[key] = pkv.kvShardsVersion[key]
	}
	args := ReceiveShardsOpsArgs{Ops: pkv.cacheOps[:cacheOpsEnd], IsOver: false, SendGroupId: pkv.gid, ShardsVersion: shardsVersion}
	pkv.mu.Unlock()
	util.Debug(util.DebugMoveShardState, pkv.me, "group[%v] raft[%v] try begin move cache ops[%v] to group[%v]", pkv.gid, pkv.me, args.Ops, pkv.movingGroup)
	failTime := 0
	for {
		//序列化分片
		srv := pkv.make_end(receiveServers[leader])
		var reply ReceiveShardsOpsReply
		ok := srv.Call("ShardKV.ReceiveShardsOps", &args, &reply)
		if ok {
			failTime = 0
			if reply.Err == OK {
				op := Op{Op: "moveShardsReplayCacheOpsState"}
				result := pkv.raftWait(op)
				if result.Err != OK {
					return ErrTryAgainLater
				}
				return OK
			} else if reply.Err == ErrTryAgainLater {
				return ErrTryAgainLater
			}
		} else {
			failTime++
			if failTime == 3 {
				return ErrTryAgainLater
			}
		}
		leader = (leader + 1) % len(receiveServers)
		pkv.mu.Lock()
		pkv.gid2leader[receiveGid] = leader
		pkv.mu.Unlock()
	}
}

func (ms *moveShardsReplayCacheOpsState) raftOps(pkv *ShardKV, pApplyMsg raft.ApplyMsg) OpResult {
	if pkv.movingShardsState != ms {
		util.Debug(util.DebugServerRaft, pkv.me, "group[%v] raft[%v] in raft no cache state[%v] error", pkv.gid, pkv.me, pkv.movingShardsState)
		return OpResult{Err: OK}
	}
	//TODO 去更新
	util.Debug(util.DebugServerRaft, pkv.me, "group[%v] raft[%v] in raft no cache ", pkv.gid, pkv.me)
	//op := pApplyMsg.Command.(Op)
	pkv.movingShardsState = &gMoveShardsNoCacheMoreOpsState
	return OpResult{Err: OK}
}

type moveShardsNoCacheMoreOpsState struct {
}

func (state *moveShardsNoCacheMoreOpsState) handle(pkv *ShardKV) Err {
	receiveGid := pkv.movingGroup
	receiveServers := pkv.movingServers
	leader := pkv.gid2leader[receiveGid]
	shardsVersion := make(map[int]int, len(pkv.movingShards))
	for key, _ := range pkv.movingShards {
		shardsVersion[key] = pkv.kvShardsVersion[key]
	}
	args := ReceiveShardsOpsArgs{Ops: pkv.cacheOps[pkv.noMoreCacheOpsBegin:], IsOver: true, SendGroupId: pkv.gid, ShardsVersion: shardsVersion}
	pkv.mu.Unlock()
	util.Debug(util.DebugMoveShardState, pkv.me, "group[%v] raft[%v] try begin move else cache ops[%v] to group[%v]", pkv.gid, pkv.me, args.Ops, pkv.movingGroup)
	for {
		//序列化分片
		srv := pkv.make_end(receiveServers[leader])
		var reply ReceiveShardsOpsReply
		ok := srv.Call("ShardKV.ReceiveShardsOps", &args, &reply)
		if ok && reply.Err == OK {
			op := Op{Op: "moveShardsNoCacheMoreOpsState"}
			result := pkv.raftWait(op)
			if result.Err != OK {
				return ErrTryAgainLater
			}
			return OK
		}
		if ok && reply.Err == ErrTryAgainLater {
			return ErrTryAgainLater
		}
		leader = (leader + 1) % len(receiveServers)
		pkv.mu.Lock()
		pkv.gid2leader[receiveGid] = leader
		pkv.mu.Unlock()
	}
}
func (ms *moveShardsNoCacheMoreOpsState) raftOps(pkv *ShardKV, pApplyMsg raft.ApplyMsg) OpResult {
	if pkv.movingShardsState != ms {
		return OpResult{Err: OK}
	}
	//做一些清理操作
	for key, _ := range pkv.movingShards {
		delete(pkv.kvShards, key)
	}
	//删除索引
	util.Debug(util.DebugServerRaft, pkv.me, "group[%v] raft[%v] in raft delete moving shards shards[%v]", pkv.gid, pkv.me, pkv.kvShards)

	pkv.movingShards = make(map[int]KVShard)
	pkv.cacheOps = make([]KVOp, 0)
	pkv.movingShardsState = &gNoMoveShardsState
	return OpResult{Err: OK}
}

type receiveShardsSnapshotOperator struct {
}

func (ms *receiveShardsSnapshotOperator) raftOps(pkv *ShardKV, pApplyMsg raft.ApplyMsg) OpResult {
	op := pApplyMsg.Command.(Op)
	for key, value := range op.KVShards {
		if op.ShardsVersion[key] <= pkv.kvShardsVersion[key] {
			continue
		}
		if _, ok := pkv.kvShards[key]; ok {
			continue
		}
		//如果已经在接收中，则可能已经开始
		if _, ok := pkv.waitReceiveShard[key]; ok {
			continue
		}
		copyOneShard := KVShard{}
		copyOneShard.FilterTable = make(map[int64]OpResult)
		copyOneShard.MemTable = make(map[string]string)
		for key, value := range value.MemTable {
			copyOneShard.MemTable[key] = value
		}
		for key, value := range value.FilterTable {
			copyOneShard.FilterTable[key] = value
		}
		pkv.waitReceiveShard[key] = copyOneShard
		pkv.waitReceiveShardState[key] = ReceiveShardSnapshot
		pkv.kvShardsVersion[key] = op.ShardsVersion[key]
	}
	util.Debug(util.DebugMoveShardState, pkv.me, "group[%v] raft[%v] receive snapshot result[%v] from group[%v]", pkv.gid, pkv.me, pkv.waitReceiveShard, op.SendGroupId)
	return OpResult{Err: OK}
}

type receiveShardsOpsOperator struct {
}

//TODO 这里做的不够优雅，可以给一个编号，绑定一组shards，这样一个编号有一个状态便可，不用每个分片有一个状态

//TODO 代码越乱的地方，越容易出错
func (ms *receiveShardsOpsOperator) raftOps(pkv *ShardKV, pApplyMsg raft.ApplyMsg) OpResult {
	op := pApplyMsg.Command.(Op)
	// TODO 待修改

	// 对ShardsVersion做过滤
	tShardsVersion := make(map[int]int, 0)
	for key, value := range op.ShardsVersion {
		if value != pkv.kvShardsVersion[key] {
			util.Debug(util.DebugMoveShardState, pkv.me, "group[%v] raft[%v] delete key[%v] args.version[%v] pkv.version[%v]", pkv.gid, pkv.me, op, key, value, pkv.kvShardsVersion)
			continue
		}
		tShardsVersion[key] = value
	}
	for _, oneOp := range op.CacheOps {
		shardId := key2shard(oneOp.Key)
		if _, ok := tShardsVersion[shardId]; !ok {
			continue
		}
		//主要用来防止重放
		if state := pkv.waitReceiveShardState[shardId]; !op.IsOver && state == ReceiveShardCacheOps {
			util.Debug(util.DebugMoveShardState, pkv.me, "group[%v] raft[%v] shardid[%v] state not success[%v]", pkv.gid, pkv.me, shardId, pkv.waitReceiveShardState)
			continue
		}
		kvShard, ok := pkv.waitReceiveShard[shardId]
		if !ok {
			util.Debug(util.DebugMoveShardState, pkv.me, "group[%v] raft[%v] shardid[%v] not int waitReceive[%v]", pkv.gid, pkv.me, shardId, pkv.waitReceiveShard)
			continue
		}
		//util.Debug(util.DebugMoveShardState, "group[%v] raft[%v] oneOps[%v] shardid[%v] from group[%v]", pkv.gid, pkv.me, oneOp, shardId, op.SendGroupId)
		if result, ok := kvShard.FilterTable[oneOp.Sequence.ClientId]; !ok || result.Seq != oneOp.Sequence {
			switch oneOp.Op {
			case "Put":
				kvShard.MemTable[oneOp.Key] = oneOp.Value
			case "Append":
				value := kvShard.MemTable[oneOp.Key]
				kvShard.MemTable[oneOp.Key] = value + oneOp.Value
			}
			result.Seq = oneOp.Sequence
			result.Err = OK
			kvShard.FilterTable[oneOp.Sequence.ClientId] = result
		}
	}

	util.Debug(util.DebugMoveShardState, pkv.me, "group[%v] raft[%v] receive ops[%v] result[%v]", pkv.gid, pkv.me, op, pkv.waitReceiveShard)
	if op.IsOver {
		for key, _ := range tShardsVersion {
			oneShard, ok := pkv.waitReceiveShard[key]
			if !ok {
				continue
			}
			copyOneShard := KVShard{}
			copyOneShard.FilterTable = make(map[int64]OpResult)
			copyOneShard.MemTable = make(map[string]string)
			for key, value := range oneShard.MemTable {
				copyOneShard.MemTable[key] = value
			}
			for key, value := range oneShard.FilterTable {
				copyOneShard.FilterTable[key] = value
			}
			pkv.kvShards[key] = copyOneShard
			delete(pkv.waitReceiveShard, key)
			delete(pkv.waitReceiveShardState, key)
			if pkv.config.Shards[key] != pkv.gid {
				if _, ok := pkv.group2WaitMoveShard[pkv.config.Shards[key]]; !ok {
					pkv.group2WaitMoveShard[pkv.config.Shards[key]] = make(map[int]bool)
				}
				pkv.group2WaitMoveShard[pkv.config.Shards[key]][key] = true
			}
		}
		util.Debug(util.DebugMoveShardState, pkv.me, "group[%v] kv[%v] in receive ops shards[%v] from group[%v]", pkv.gid, pkv.me, pkv.kvShards, op.SendGroupId)
	} else {
		util.Debug(util.DebugMoveShardState, pkv.me, "group[%v] kv[%v] shardsVersion[%v] become ReceiveShardCacheOps", pkv.gid, pkv.me, tShardsVersion)
		for key, _ := range tShardsVersion {
			if _, ok := pkv.waitReceiveShardState[key]; ok {
				pkv.waitReceiveShardState[key] = ReceiveShardCacheOps
			}
		}
	}
	return OpResult{Err: OK}
}
