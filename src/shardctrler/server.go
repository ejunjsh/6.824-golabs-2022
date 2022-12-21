package shardctrler


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "6.824/labgob"
import "time"
import "log"
import "sort"


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	killCh  chan struct{}
	notifyChes map[int64]chan NotifyMsg
	clientLastSeqNum map[int64]int64

	configs []Config // indexed by config num
}


type Op struct {
	// Your data here.
	SeqNum    int64
	NotifyId    int64
	Args     interface{}
	Method   string
	ClientId int64
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		SeqNum:    args.SeqNum,
		NotifyId:    nrand(),
		Args:     *args,
		Method:   "Join",
		ClientId: args.ClientId,
	}
	res := sc.start(op)
	reply.Err = res.Err
	reply.WrongLeader = res.WrongLeader
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		SeqNum:    args.SeqNum,
		NotifyId:    nrand(),
		Args:     *args,
		Method:   "Leave",
		ClientId: args.ClientId,
	}
	res := sc.start(op)
	reply.Err = res.Err
	reply.WrongLeader = res.WrongLeader
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		SeqNum:    args.SeqNum,
		NotifyId:    nrand(),
		Args:     *args,
		Method:   "Move",
		ClientId: args.ClientId,
	}
	res := sc.start(op)
	reply.Err = res.Err
	reply.WrongLeader = res.WrongLeader
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()
	if args.Num > 0 && args.Num < len(sc.configs) {
		reply.Err = OK
		reply.WrongLeader = false
		reply.Config = sc.getConfigByIndex(args.Num)
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	op := Op{
		SeqNum:    args.SeqNum,
		NotifyId:    nrand(),
		Args:     *args,
		Method:   "Query",
		ClientId: args.ClientId,
	}
	res := sc.start(op)
	reply.Err = res.Err
	reply.WrongLeader = res.WrongLeader
	reply.Config = res.Config
}


//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
	close(sc.killCh)
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(Config{})
	labgob.Register(QueryArgs{})
	labgob.Register(QueryReply{})
	labgob.Register(JoinArgs{})
	labgob.Register(JoinReply{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(LeaveReply{})
	labgob.Register(MoveReply{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.clientLastSeqNum = make(map[int64]int64)
	sc.killCh = make(chan struct{})
	sc.notifyChes = make(map[int64]chan NotifyMsg)

	go sc.applier()

	return sc
}

func (sc *ShardCtrler) getConfigByIndex(idx int) Config {
	if idx < 0 || idx >= len(sc.configs) {
		return sc.configs[len(sc.configs)-1].Copy()
	} else {
		return sc.configs[idx].Copy()
	}
}

func (sc *ShardCtrler) applier(){
	for {
		select {
		case <-sc.killCh:
			return
		case msg := <-sc.applyCh:
			if msg.CommandValid{
				op := msg.Command.(Op)

				sc.mu.Lock()

				switch op.Method {
				case "Join":
					if v, ok:=sc.clientLastSeqNum[op.ClientId];!ok || v < op.SeqNum {
						sc.join(op.Args.(JoinArgs))
						sc.clientLastSeqNum[op.ClientId] = op.SeqNum
					}
				case "Leave":
					if v, ok:=sc.clientLastSeqNum[op.ClientId];!ok || v < op.SeqNum {
						sc.leave(op.Args.(LeaveArgs))
						sc.clientLastSeqNum[op.ClientId] = op.SeqNum
					}
				case "Move":
					if v, ok:=sc.clientLastSeqNum[op.ClientId];!ok || v < op.SeqNum {
						sc.move(op.Args.(MoveArgs))
						sc.clientLastSeqNum[op.ClientId] = op.SeqNum
					}
				case "Query":
				default:
					log.Fatalf("unknown method: %s", op.Method)
				}
				res := NotifyMsg{
					Err:         OK,
					WrongLeader: false,
				}
				if op.Method == "Query" {
				   res.Config = sc.getConfigByIndex(op.Args.(QueryArgs).Num)
				}
				if ch, ok := sc.notifyChes[op.NotifyId]; ok {
					go func(){
						t := time.NewTimer(time.Millisecond * 50000)
						defer t.Stop()
						select{
							case ch <- res :
							case <-t.C:
						}
					}()
				}
				sc.mu.Unlock()
			}
		}
	}
}

func (sc *ShardCtrler) start(op Op) (res NotifyMsg){
	_, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		res.WrongLeader = true
		return
	}

	sc.mu.Lock()
	ch := make(chan NotifyMsg, 1)
	sc.notifyChes[op.NotifyId] = ch
	sc.mu.Unlock()

	t := time.NewTimer(time.Millisecond * 300)
	defer t.Stop()
	select {
	case res = <-ch:
		sc.mu.Lock()
		delete(sc.notifyChes,op.NotifyId)
		sc.mu.Unlock()
		return
	case <-t.C:
		sc.mu.Lock()
		delete(sc.notifyChes,op.NotifyId)
		sc.mu.Unlock()
		res.Err = ErrWrongLeader
		res.WrongLeader = true
		return
	}
}

func (sc *ShardCtrler) join(args JoinArgs) {
	config := sc.getConfigByIndex(-1)
	config.Num += 1

	for k, v := range args.Servers {
		config.Groups[k] = v
	}

	sc.adjustConfig(&config)
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) leave(args LeaveArgs) {
	config := sc.getConfigByIndex(-1)
	config.Num += 1

	for _, gid := range args.GIDs {
		delete(config.Groups, gid)
		for i, v := range config.Shards {
			if v == gid {
				config.Shards[i] = 0
			}
		}
	}
	sc.adjustConfig(&config)
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) move(args MoveArgs) {
	config := sc.getConfigByIndex(-1)
	config.Num += 1
	config.Shards[args.Shard] = args.GID
	sc.configs = append(sc.configs, config)
}

func (sc *ShardCtrler) adjustConfig(config *Config) {
	if len(config.Groups) == 0 {
		config.Shards = [NShards]int{}
	} else if len(config.Groups) == 1 {
		// set shards one gid
		for k, _ := range config.Groups {
			for i, _ := range config.Shards {
				config.Shards[i] = k
			}
		}
	} else if len(config.Groups) <= NShards {
		avg := NShards / len(config.Groups)
		// 每个 gid 分 avg 个 shard
		otherShardsCount := NShards - avg*len(config.Groups)
		needLoop := false
		lastGid := 0

	LOOP:
		var keys []int
		for k := range config.Groups {
			keys = append(keys, k)
		}
		sort.Ints(keys)
		for _, gid := range keys {
			lastGid = gid
			count := 0
			// 先 count 已有的
			for _, val := range config.Shards {
				if val == gid {
					count += 1
				}
			}

			// 判断是否需要改变
			if count == avg {
				continue
			} else if count > avg && otherShardsCount == 0 {
				// 减少到 avg
				c := 0
				for i, val := range config.Shards {
					if val == gid {
						if c == avg {
							config.Shards[i] = 0
						} else {
							c += 1
						}
					}
				}

			} else if count > avg && otherShardsCount > 0 {
				// 减到 othersShardsCount 为 0
				// 若还 count > avg, set to 0
				c := 0
				for i, val := range config.Shards {
					if val == gid {
						if c == avg+otherShardsCount {
							config.Shards[i] = 0
						} else {
							if c == avg {
								otherShardsCount -= 1
							} else {
								c += 1
							}

						}
					}
				}

			} else {
				// count < avg, 此时有可能没有位置
				for i, val := range config.Shards {
					if count == avg {
						break
					}
					if val == 0 && count < avg {
						config.Shards[i] = gid
					}
				}

				if count < avg {
					needLoop = true
				}

			}

		}

		if needLoop {
			needLoop = false
			goto LOOP
		}

		// 可能每一个 gid 都 >= avg，但此时有空的 shard
		if lastGid != 0 {
			for i, val := range config.Shards {
				if val == 0 {
					config.Shards[i] = lastGid
				}
			}
		}

	} else {
		// len(config.Groups) > NShards
		// 每个 gid 最多一个， 会有空余 gid
		gids := make(map[int]int)
		emptyShards := make([]int, 0, NShards)
		for i, gid := range config.Shards {
			if gid == 0 {
				emptyShards = append(emptyShards, i)
				continue
			}
			if _, ok := gids[gid]; ok {
				emptyShards = append(emptyShards, i)
				config.Shards[i] = 0
			} else {
				gids[gid] = 1
			}
		}
		n := 0
		if len(emptyShards) > 0 {
			var keys []int
			for k := range config.Groups {
				keys = append(keys, k)
			}
			sort.Ints(keys)
			for _, gid := range keys {
				if _, ok := gids[gid]; !ok {
					config.Shards[emptyShards[n]] = gid
					n += 1
				}
				if n >= len(emptyShards) {
					break
				}
			}
		}

	}

}