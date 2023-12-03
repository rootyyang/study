package shardctrler

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	// Your definitions here.
	cancelFunc    context.CancelFunc
	cancelContext context.Context

	filterTable map[int64]SequenceAndOpResult

	lastIndex    int
	index2result map[int]SequenceAndRespChannel

	//3B
	persister *raft.Persister

	configs []Config // indexed by config num

}

type Op struct {
	// Your data here.
	Op       string
	Sequence UniqSeq
	// For Query
	Num int
	// For Join
	Servers map[int][]string // new GID -> servers mappings
	// For Leave
	GIDs []int
	// For Move
	Shard int
	GID   int //Allocation 复用

	//Allocation
	Shards []int
}

type SequenceAndOpResult struct {
	Seq int
	OpResult
}

type OpResult struct {
	Err            Err
	Config         Config
	AllocateShards []int
}
type SequenceAndRespChannel struct {
	sequence    UniqSeq
	respChannel chan OpResult
}

func (sc *ShardCtrler) CommonHandle(pOp Op) (rOpResult OpResult) {
	sc.mu.Lock()
	if result, ok := sc.filterTable[pOp.Sequence.ClientId]; ok && result.Seq == pOp.Sequence.Seq {
		rOpResult = result.OpResult
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()
	index, _, isLeader := sc.rf.Start(pOp)
	if !isLeader {
		//fmt.Printf("kv[%v] not leader \n", sc.me)
		rOpResult = OpResult{Err: WrongLeaderErr}
		return
	}
	//fmt.Printf("kv[%v] Get start[%v] op[%+v]\n", sc.me, index, pOp)
	respChannel := make(chan OpResult)
	sc.mu.Lock()
	if lastChannel, ok := sc.index2result[index]; ok {
		lastChannel.respChannel <- OpResult{Err: WrongLeaderErr}
	}
	sc.index2result[index] = SequenceAndRespChannel{sequence: pOp.Sequence, respChannel: respChannel}
	sc.mu.Unlock()
	timeoutTimer := time.NewTimer(100 * time.Millisecond)
	select {
	case opResult := <-respChannel:
		rOpResult = opResult
	case <-timeoutTimer.C:
		sc.mu.Lock()
		if resp, ok := sc.index2result[index]; ok && resp.sequence == pOp.Sequence {
			delete(sc.index2result, index)
		}
		sc.mu.Unlock()
		rOpResult = OpResult{Err: TimeOutErr}
	}
	return
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{Op: "Join", Sequence: args.Sequence, Servers: args.Servers}
	rOpResult := sc.CommonHandle(op)
	if rOpResult.Err != OK {
		*reply = JoinReply{WrongLeader: true, Err: rOpResult.Err}
		return
	}
	*reply = JoinReply{WrongLeader: false, Err: rOpResult.Err}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{Op: "Leave", Sequence: args.Sequence, GIDs: args.GIDs}
	rOpResult := sc.CommonHandle(op)
	if rOpResult.Err != OK {
		*reply = LeaveReply{WrongLeader: true, Err: rOpResult.Err}
		return
	}
	*reply = LeaveReply{WrongLeader: false, Err: rOpResult.Err}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{Op: "Move", Sequence: args.Sequence, Shard: args.Shard, GID: args.GID}
	rOpResult := sc.CommonHandle(op)
	if rOpResult.Err != OK {
		*reply = MoveReply{WrongLeader: true, Err: rOpResult.Err}
		return
	}
	*reply = MoveReply{WrongLeader: false, Err: rOpResult.Err}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{Op: "Query", Sequence: args.Sequence, Num: args.Num}
	rOpResult := sc.CommonHandle(op)
	if rOpResult.Err != OK {
		*reply = QueryReply{WrongLeader: true, Err: rOpResult.Err}
		return
	}
	*reply = QueryReply{WrongLeader: false, Config: rOpResult.Config}

}

/*
func (sc *ShardCtrler) AllocateShard(args *AllocateShardArgs, reply *AllocateShardReply) {
	// Your code here.
	op := Op{Op: "AllocateShard", Sequence: args.Sequence, Shards: args.Shards, GID: args.GID}
	rOpResult := sc.CommonHandle(op)
	if rOpResult.Err != OK {
		*reply = AllocateShardReply{WrongLeader: true}
		return
	}
	*reply = AllocateShardReply{WrongLeader: false, Err: rOpResult.Err}
}*/

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	sc.cancelFunc()
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applier() {
	for {
		select {
		case <-sc.cancelContext.Done():
			return
		case applyMsg := <-sc.applyCh:
			if applyMsg.CommandValid {
				op := applyMsg.Command.(Op)
				//fmt.Printf("kv[%v] Applier [%v]\n", sc.me, applyMsg.CommandIndex)
				sc.mu.Lock()
				//清理过期的filtertable
				var result SequenceAndOpResult
				var ok bool

				sc.lastIndex = applyMsg.CommandIndex
				if result, ok = sc.filterTable[op.Sequence.ClientId]; !ok || result.Seq != op.Sequence.Seq {
					//TODO添加内容
					result.Err = OK
					switch op.Op {
					case "Query":
						if op.Num < 0 || op.Num >= len(sc.configs) {
							result.OpResult.Config = sc.configs[len(sc.configs)-1]
						} else {
							result.OpResult.Config = sc.configs[op.Num]
						}
					case "Move":
						config := sc.configs[len(sc.configs)-1]
						config.Shards[op.Shard] = op.GID
						config.Num++
						sc.configs = append(sc.configs, config)
						//fmt.Printf("master[%v] move applier config[%+v] \n", sc.me, sc.configs)
					case "Join":
						config := sc.configs[len(sc.configs)-1]
						newConfig, err := JoinGroups(config, op.Servers)
						if err == nil {
							sc.configs = append(sc.configs, newConfig)
						}
						//fmt.Printf("master[%v] join applier config[%+v] \n", sc.me, sc.configs)
					case "Leave":
						config := sc.configs[len(sc.configs)-1]
						newConfig, err := LeaveGroups(config, op.GIDs)
						if err == nil {
							sc.configs = append(sc.configs, newConfig)
						}
					/*case "AllocateShard":
					//fmt.Printf("master[%v] leave applier config[%+v] \n", sc.me, sc.configs)
					config := sc.configs[len(sc.configs)-1]
					result.AllocateShards = make([]int, 0)
					for _, value := range op.Shards {
						if !config.ShardAllocate[value] && config.Shards[value] == op.GID {
							result.AllocateShards = append(result.AllocateShards, value)
						}
					}
					if len(result.AllocateShards) != 0 {
						newConfig := config
						newConfig.Groups = make(map[int][]string, len(newConfig.Groups))
						for key, value := range config.Groups {
							newConfig.Groups[key] = value
						}
						for _, value := range result.AllocateShards {
							newConfig.ShardAllocate[value] = true
						}
						sc.configs = append(sc.configs, newConfig)
					}*/
					default:
						fmt.Printf("Fatal Err Wrong Op[%v]\n", op.Op)
					}
					result.Seq = op.Sequence.Seq
					sc.filterTable[op.Sequence.ClientId] = result
					//fmt.Printf("master[%v] applier apply index[%v] op[%+v] \n", sc.me, applyMsg.CommandIndex, op)

				}
				resp, ok := sc.index2result[applyMsg.CommandIndex]
				if ok {
					delete(sc.index2result, applyMsg.CommandIndex)
				}
				sc.mu.Unlock()
				if ok {
					if resp.sequence == op.Sequence {
						resp.respChannel <- result.OpResult
					} else {
						resp.respChannel <- OpResult{Err: WrongLeaderErr}
					}
				}
			}
		}
	}
}

//要求:1.搬迁最少, 2.最均衡
//了解一下业界相关的算法
func JoinGroups(pOldConfig Config, pJoinGroup map[int][]string) (Config, error) {
	//判断，对实际需要添加的节点进行过滤
	for key, _ := range pJoinGroup {
		if _, ok := pOldConfig.Groups[key]; ok {
			delete(pJoinGroup, key)
		}
	}
	if len(pJoinGroup) == 0 {
		return pOldConfig, fmt.Errorf("no need join group")
	}
	//拷贝map
	oldGroups := pOldConfig.Groups
	pOldConfig.Groups = make(map[int][]string, len(oldGroups))
	for key, value := range oldGroups {
		pOldConfig.Groups[key] = value
	}
	for key, value := range pJoinGroup {
		pOldConfig.Groups[key] = value
	}

	rebalance(&pOldConfig)
	return pOldConfig, nil
}

func LeaveGroups(pOldConfig Config, pLeaveGroups []int) (Config, error) {
	leaveGroups := make([]int, 0, len(pLeaveGroups))
	//不存在的，做过滤
	for _, value := range pLeaveGroups {
		if _, ok := pOldConfig.Groups[value]; ok {
			leaveGroups = append(leaveGroups, value)
		}
	}
	if len(leaveGroups) == 0 {
		return pOldConfig, fmt.Errorf("no need join group")
	}
	//拷贝map，由于map是引用数据结构，所以需要拷贝map
	oldGroups := pOldConfig.Groups
	pOldConfig.Groups = make(map[int][]string, len(oldGroups))
	for key, value := range oldGroups {
		pOldConfig.Groups[key] = value
	}
	for _, value := range pLeaveGroups {
		delete(pOldConfig.Groups, value)
	}
	rebalance(&pOldConfig)
	return pOldConfig, nil
}
func rebalance(pOldConfig *Config) {
	pOldConfig.Num++
	if len(pOldConfig.Groups) == 0 {
		return
	}
	group2Shards := make(map[int][]int)
	needMoveShards := make([]int, 0)
	for key, value := range pOldConfig.Shards {
		if _, ok := pOldConfig.Groups[value]; !ok {
			needMoveShards = append(needMoveShards, key)
			continue
		}
		if group2Shards[value] == nil {
			group2Shards[value] = make([]int, 0)
		}
		group2Shards[value] = append(group2Shards[value], key)
	}
	//对groupid排序，否则不一定可重放
	groupSlice := make([]int, 0, len(group2Shards))
	for key, _ := range pOldConfig.Groups {
		groupSlice = append(groupSlice, key)
	}
	sort.Ints(groupSlice)

	everyGroupAtLeastNum := len(pOldConfig.Shards) / (len(pOldConfig.Groups))
	hasMoreSharedNum := len(pOldConfig.Shards) % (len(pOldConfig.Groups))

	//收集每个节点多余的分片
	firstAllocationHasMoreSharedNum := hasMoreSharedNum
	for _, groupId := range groupSlice {
		shouldHasShardNum := everyGroupAtLeastNum
		if firstAllocationHasMoreSharedNum > 0 {
			shouldHasShardNum++
			firstAllocationHasMoreSharedNum--
		}
		groupShards := group2Shards[groupId]
		if len(groupShards) > shouldHasShardNum {
			needMoveShards = append(needMoveShards, groupShards[shouldHasShardNum:]...)
			group2Shards[groupId] = groupShards[:shouldHasShardNum]
		}
	}
	sort.Ints(needMoveShards)
	//实际开始分配
	beginPos := 0
	for _, groupId := range groupSlice {
		//if group2Shards
		shouldHasShardNum := everyGroupAtLeastNum
		if hasMoreSharedNum > 0 {
			shouldHasShardNum++
			hasMoreSharedNum--
		}
		groupShards := group2Shards[groupId]
		if len(groupShards) < shouldHasShardNum {
			endPos := beginPos + shouldHasShardNum - len(groupShards)
			for _, value := range needMoveShards[beginPos:endPos] {
				pOldConfig.Shards[value] = groupId
			}
			beginPos = endPos
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}
	/*for key, _ := range sc.configs[0].ShardAllocate {
		sc.configs[0].ShardAllocate[key] = false
	}*/

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	//sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.persister = persister
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	sc.filterTable = make(map[int64]SequenceAndOpResult)
	sc.index2result = make(map[int]SequenceAndRespChannel)
	sc.cancelContext, sc.cancelFunc = context.WithCancel(context.Background())

	go sc.applier()
	//go sc.snapshotRoute()
	return sc
}
