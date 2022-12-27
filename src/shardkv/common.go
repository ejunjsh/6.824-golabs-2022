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
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string


type CommonArgs struct{
	ClientId int64
	SeqNum int64
	ConfigId int
}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommonArgs
}

func (c *PutAppendArgs) copy() PutAppendArgs {
	r := PutAppendArgs{
		Key:       c.Key,
		Value:     c.Value,
		Op:        c.Op,
		CommonArgs: CommonArgs{
			ClientId:  c.ClientId,
			SeqNum:     c.SeqNum,
			ConfigId: c.ConfigId,
		},
	}
	return r
}


type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	CommonArgs
}

func (c *GetArgs) copy() GetArgs {
	r := GetArgs{
		Key:       c.Key,
		CommonArgs: CommonArgs{
			ClientId:  c.ClientId,
			SeqNum:     c.SeqNum,
			ConfigId: c.ConfigId,
		},

	}
	return r
}

type GetReply struct {
	Err   Err
	Value string
}

type CommonShardArgs struct{
	ConfigId int
	ShardId  int
}

type FetchShardDataArgs struct {
	CommonShardArgs
}

type FetchShardDataReply struct {
	Success    bool
	ClientLastSeqNum map[int64]int64
	Data       map[string]string
}

func (reply *FetchShardDataReply) Copy() FetchShardDataReply {
	res := FetchShardDataReply{
		Success:    reply.Success,
		Data:       make(map[string]string),
		ClientLastSeqNum: make(map[int64]int64),
	}
	for k, v := range reply.Data {
		res.Data[k] = v
	}
	for k, v := range reply.ClientLastSeqNum {
		res.ClientLastSeqNum[k] = v
	}
	return res
}

type CleanShardDataArgs struct {
	CommonShardArgs
}

type CleanShardDataReply struct {
	Success bool
}

type ShardData struct {
	CommonShardArgs
	ClientLastSeqNum map[int64]int64
	Data       map[string]string
}

type NotifyMsg struct {
	Err Err
	Value string
}
