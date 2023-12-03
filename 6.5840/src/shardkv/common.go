package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                    = "OK"
	ErrNoKey              = "ErrNoKey"
	ErrTimeOut            = "ErrTimeOut"
	ErrWrongGroup         = "ErrWrongGroup"
	ErrWrongLeader        = "ErrWrongLeader"
	ErrTryAgainLater      = "ErrTryAgainLater"
	ErrConfigCannotUpdate = "ErrConfigCannotUpdate"
	ErrShardNotExist      = "ErrShardNotExist"
	ErrFatel              = "ErrFatel"
)

type MoveShardStage int

const (
	//阶段一接收snapshot
	MoveSnapshot = iota
	//阶段二按顺序执行所有的snapshot
	MoveOps
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Sequence UniqSeq
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Sequence UniqSeq
}

type GetReply struct {
	Err   Err
	Value string
}

//接收snapshot
type ReceiveShardsSnapshotArgs struct {
	SendGroupId   int
	KVShards      map[int]KVShard
	ConfigNum     int
	ShardsVersion map[int]int
}

type ReceiveShardsSnapshotReply struct {
	Err Err
}
type ReceiveShardsOpsArgs struct {
	SendGroupId   int
	IsOver        bool //是否是最后一批Ops
	Ops           []KVOp
	ShardsVersion map[int]int
}
type ReceiveShardsOpsReply struct {
	Err Err
}

type UniqSeq struct {
	ClientId int64
	Seq      int
}
