package shardkv

import (
	"6.5840/raft"
	"6.5840/util"
)

func init() {
	registerRaftOperator("Get", &dataOperator{})
	registerRaftOperator("Put", &dataOperator{})
	registerRaftOperator("Append", &dataOperator{})

}

type dataOperator struct {
}

func (do *dataOperator) raftOps(pkv *ShardKV, pApplyMsg raft.ApplyMsg) OpResult {
	op := pApplyMsg.Command.(Op)
	// TODO 待修改
	shardId := key2shard(op.Key)
	var result OpResult
	kvShard, ok := pkv.kvShards[shardId]
	if !ok {
		if pkv.config.Shards[shardId] != pkv.gid {
			result = OpResult{Err: ErrWrongGroup}
			return result
		}
		result = OpResult{Err: ErrTryAgainLater}
		return result
	}
	//搬迁中，且状态为
	if _, ok := pkv.movingShards[shardId]; ok && pkv.movingShardsState == &gMoveShardsNoCacheMoreOpsState {
		result = OpResult{Err: ErrWrongGroup}
		return result
	}
	//这里重新对result做了赋值
	if result, ok = kvShard.FilterTable[op.Sequence.ClientId]; !ok || result.Seq != op.Sequence {
		switch op.Op {
		case "Get":
			result.Value = kvShard.MemTable[op.Key]
		case "Put":
			kvShard.MemTable[op.Key] = op.Value
			//result.Value = op.Value
			if _, ok := pkv.movingShards[shardId]; ok {
				pkv.cacheOps = append(pkv.cacheOps, KVOp{Op: op.Op, Key: op.Key, Value: op.Value, Sequence: op.Sequence})
			}
		case "Append":
			value := kvShard.MemTable[op.Key]
			kvShard.MemTable[op.Key] = value + op.Value
			//result.Value = value + op.Value
			if _, ok := pkv.movingShards[shardId]; ok {
				pkv.cacheOps = append(pkv.cacheOps, KVOp{Op: op.Op, Key: op.Key, Value: op.Value, Sequence: op.Sequence})
			}
		}
		result.Seq = op.Sequence
		result.Err = OK
		kvShard.FilterTable[op.Sequence.ClientId] = result
	}
	if op.Op != "Get" {
		util.Debug(util.DebugKV, pkv.me, "group[%v] raft[%v] op[%v] key[%v] value[%v] in applier result[%v] shards[%v]", pkv.gid, pkv.me, op.Op, op.Key, op.Value, result, pkv.kvShards)
	}
	return result
}
