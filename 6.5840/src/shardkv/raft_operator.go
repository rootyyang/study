package shardkv

import (
	"fmt"

	"6.5840/raft"
	"6.5840/util"
)

//注意，全局变量对于所有的shardkv对象是共享的，所以要特别小心，不要共享数据
func init() {
	registerRaftOperator("configUpdate", &configUpdateOperator{})
}

var opFactoryMap = map[string]operator{}

func registerRaftOperator(pName string, pOperator operator) {
	if _, ok := opFactoryMap[pName]; ok {
		fmt.Printf("Fatel register raft operator[%v]\n", pName)
		return
	}
	opFactoryMap[pName] = pOperator
}

func operatorFactory(pOp string) operator {
	if value, ok := opFactoryMap[pOp]; ok {
		return value
	}
	return nil
}

type operator interface {
	raftOps(*ShardKV, raft.ApplyMsg) OpResult
}

type configUpdateOperator struct {
}

func (co *configUpdateOperator) raftOps(pkv *ShardKV, pApplyMsg raft.ApplyMsg) OpResult {
	op := pApplyMsg.Command.(Op)
	if pkv.config.Num+1 != op.NewConfig.Num {
		//不做更新，跳过该操作
		return OpResult{Err: ErrConfigCannotUpdate}
	}
	pkv.config = op.NewConfig
	util.Debug(util.DebugConfig, pkv.me, "group[%v] raft[%v] config update[%v]", pkv.gid, pkv.me, pkv.config)
	// TODO 这里可以增加一个allocation标识，而不是感知config.Num
	if pkv.config.Num == 1 {
		for key, value := range pkv.config.Shards {
			if value == pkv.gid {
				shard := KVShard{}
				shard.MemTable = make(map[string]string)
				shard.FilterTable = make(map[int64]OpResult)
				pkv.kvShards[key] = shard
			}
			pkv.kvShardsVersion[key] = 0
		}
	}
	// 将需要搬迁的数据整理出来
	// 这里可以使用观察者模式，注册一些需要感知到config变化的函数，这样这个函数便不会这么突兀
	group2WaitMoveShard := make(map[int]map[int]bool, 0)
	for key, _ := range pkv.kvShards {
		//需要搬迁，且不在搬迁中
		if pkv.config.Shards[key] != pkv.gid {
			//如果没有在搬迁中(搬迁中的分片要继续搬迁完成)
			if _, ok := pkv.movingShards[key]; !ok {
				if _, ok := group2WaitMoveShard[pkv.config.Shards[key]]; !ok {
					group2WaitMoveShard[pkv.config.Shards[key]] = make(map[int]bool)
				}
				group2WaitMoveShard[pkv.config.Shards[key]][key] = true
			}
		}
	}
	pkv.group2WaitMoveShard = group2WaitMoveShard
	return OpResult{Err: OK}
}
