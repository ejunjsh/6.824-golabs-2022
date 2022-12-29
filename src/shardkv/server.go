package shardkv

import (
	"sync"
	"time"

	"bytes"
	"log"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"6.824/shardctrler"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

const (
	PullConfigInterval       = time.Millisecond * 100
	PullShardDataInterval       = time.Millisecond * 200
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key string
	Value string
	Op string
	CommonArgs
	NotifyId int64
}

type shardStatus struct{
	own map[int]bool
	expire map[int]map[int]ShardData
	wait map[int]bool 
}

type shardConfig struct{
	current shardctrler.Config
	previous shardctrler.Config
}

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
	MemoryStorage

	persister *raft.Persister

	notifyChes map[int64]chan NotifyMsg

	clientLastSeqNum [shardctrler.NShards]map[int64]int64

	pullConfigTimer *time.Timer
	pullShardDataTimer *time.Timer

	dead           int32
	killCh         chan struct{}

	mck            *shardctrler.Clerk

	shardStatus

	shardConfig
}



type MemoryStorage [shardctrler.NShards]map[string]string

func (ms MemoryStorage) get(key string)(err Err, value string){
	if v, ok := ms[key2shard(key)][key]; ok {
		err = OK
		value = v
		return
	} else {
		err = ErrNoKey
		return
	}
}

func (ms MemoryStorage) set(key,value string){
	ms[key2shard(key)][key] = value
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		CommonArgs: args.CommonArgs,
		NotifyId:     nrand(),
		Key:       args.Key,
		Op:        "Get",
	}
	res := kv.start(op)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	op := Op{
		NotifyId:     nrand(),
		Key:       args.Key,
		Value:     args.Value,
		Op:        args.Op,
		CommonArgs: args.CommonArgs,
	}
	reply.Err = kv.start(op).Err
}

func (kv *ShardKV) start(op Op) (res NotifyMsg){
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := make(chan NotifyMsg, 1)
	kv.notifyChes[op.NotifyId] = ch
	kv.mu.Unlock()

	t := time.NewTimer(time.Millisecond * 300)
	defer t.Stop()
	select {
	case res = <-ch:
		kv.mu.Lock()
		delete(kv.notifyChes,op.NotifyId)
		kv.mu.Unlock()
		return
	case <-t.C:
		kv.mu.Lock()
		delete(kv.notifyChes,op.NotifyId)
		kv.mu.Unlock()
		res.Err = ErrWrongLeader
		return
	}
}

func (kv *ShardKV) FetchShardData(args *FetchShardDataArgs, reply *FetchShardDataReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ConfigId >= kv.shardConfig.current.Num {
		return
	}

	if configData, ok := kv.shardStatus.expire[args.ConfigId]; ok {
		if shardData, ok := configData[args.ShardId]; ok {
			reply.Success = true
			reply.Data = make(map[string]string)
			reply.ClientLastSeqNum = make(map[int64]int64)
			for k, v := range shardData.Data {
				reply.Data[k] = v
			}
			for k, v := range shardData.ClientLastSeqNum {
				reply.ClientLastSeqNum[k] = v
			}
		}
	}
	return
}

func (kv *ShardKV) CleanShardData(args *CleanShardDataArgs, reply *CleanShardDataReply) {
	kv.mu.Lock()

	if args.ConfigId >= kv.shardConfig.current.Num {
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	_, _, isLeader := kv.rf.Start(*args)
	if !isLeader {
		return
	}

	for i := 0; i < 10; i++ {
		kv.mu.Lock()
		exist := kv.expireDataExist(args.ConfigId, args.ShardId)
		kv.mu.Unlock()
		if !exist {
			reply.Success = true
			return
		}
		time.Sleep(time.Millisecond * 20)
	}
	return
}


func (kv *ShardKV) recover(data []byte){
	if data == nil || len(data) < 1 { 
		data = kv.persister.ReadSnapshot()
	}

	if data == nil || len(data) < 1 { 
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvData [shardctrler.NShards]map[string]string
	var clientLastSeqNum [shardctrler.NShards]map[int64]int64
	var wait map[int]bool
	var expire map[int]map[int]ShardData
	var own map[int]bool
	var config shardctrler.Config
	var oldConfig shardctrler.Config

	if d.Decode(&kvData) != nil ||
		d.Decode(&clientLastSeqNum) != nil ||
		d.Decode(&wait) != nil ||
		d.Decode(&expire) != nil ||
		d.Decode(&own) != nil ||
		d.Decode(&config) != nil ||
		d.Decode(&oldConfig) != nil {
		log.Fatal("shard kv recover err")
	} else {
		kv.MemoryStorage = kvData
		kv.clientLastSeqNum = clientLastSeqNum
		kv.shardStatus.wait = wait
		kv.shardStatus.expire = expire
		kv.shardStatus.own = own
		kv.shardConfig.current = config
		kv.shardConfig.previous = oldConfig
	}
}

func (kv *ShardKV) encodeKVData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	
	if e.Encode(kv.MemoryStorage) != nil ||
		e.Encode(kv.clientLastSeqNum) != nil ||
		e.Encode(kv.shardStatus.wait) != nil ||
		e.Encode(kv.shardStatus.expire) != nil ||
		e.Encode(kv.shardStatus.own) != nil ||
		e.Encode(kv.shardConfig.current) != nil ||
		e.Encode(kv.shardConfig.previous) != nil {
		panic("encodeKVData err")
	}

	data := w.Bytes()
	return data
}

func (kv *ShardKV) saveSnapshot(logIndex int) {
	if kv.maxraftstate == -1 {
		return
	}
	if kv.persister.RaftStateSize() < kv.maxraftstate {
		return
	}
	data := kv.encodeKVData()
	kv.rf.Snapshot(logIndex, data)
}

func (kv *ShardKV) pullConfig() {
	for {
		select {
		case <-kv.killCh:
			return
		case <-kv.pullConfigTimer.C:
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				kv.pullConfigTimer.Reset(PullConfigInterval)
				break
			}

			kv.mu.Lock()
			lastNum := kv.current.Num
			kv.mu.Unlock()

			config := kv.mck.Query(lastNum + 1)
			if config.Num == lastNum + 1 {
				kv.mu.Lock()
				if len(kv.shardStatus.wait) == 0 && kv.current.Num+1 == config.Num {
					kv.mu.Unlock()
					kv.rf.Start(config.Copy())
				} else {
					kv.mu.Unlock()
				}
			}
			kv.pullConfigTimer.Reset(PullConfigInterval)
		}
	}
}

func (kv *ShardKV) pullShardData() {
	for {
		select {
		case <-kv.killCh:
			return
		case <-kv.pullShardDataTimer.C:
			_, isLeader := kv.rf.GetState()
			if isLeader {
				kv.mu.Lock()
				for shardId, _ := range kv.shardStatus.wait {
					go func (shardId int, config shardctrler.Config)  {
						args := FetchShardDataArgs{
							CommonShardArgs: CommonShardArgs{
								ConfigId: config.Num,
								ShardId: shardId,
							},
						}
					
						for _, s := range config.Groups[config.Shards[shardId]] {
							srv := kv.make_end(s)
							reply := FetchShardDataReply{}
							if ok := srv.Call("ShardKV.FetchShardData", &args, &reply); ok {
								if reply.Success {
									kv.mu.Lock()
									if _, ok = kv.shardStatus.wait[shardId]; ok && kv.current.Num == config.Num+1 {
										replyCopy := reply.Copy()
										mergeArgs := ShardData{
											CommonShardArgs: CommonShardArgs{
												ConfigId: args.ConfigId,
												ShardId: args.ShardId,
											},
											Data:       replyCopy.Data,
											ClientLastSeqNum: replyCopy.ClientLastSeqNum,
										}
										kv.mu.Unlock()
										_, _, isLeader := kv.rf.Start(mergeArgs)
										if !isLeader {
											break
										}
									} else {
										kv.mu.Unlock()
									}
								}
							}
						}
					}(shardId,kv.previous)
				}
				kv.mu.Unlock()
			}
			kv.pullShardDataTimer.Reset(PullShardDataInterval)
		}
	}
}

func (kv *ShardKV) configReady(configNum int, key string) Err {
	if configNum == 0 || configNum != kv.current.Num {
		return ErrWrongGroup
	}
	shardId := key2shard(key)
	if _, ok := kv.shardStatus.own[shardId]; !ok {
		return ErrWrongGroup
	}
	if _, ok := kv.shardStatus.wait[shardId]; ok {
		return ErrWrongGroup
	}
	return OK
}

func (kv *ShardKV) applier() {
	for {
		select {
		case <- kv.killCh:
			return
		case msg := <-kv.applyCh:
			if msg.SnapshotValid {
				kv.mu.Lock()
				kv.recover(msg.Snapshot)
				kv.mu.Unlock()
				continue
			} else if msg.CommandValid {
				if op, ok := msg.Command.(Op); ok {
					kv.applyOp(msg, op)
				} else if config, ok := msg.Command.(shardctrler.Config); ok {
					kv.applyConfig(msg, config)
				} else if mergeData, ok := msg.Command.(ShardData); ok {
					kv.applyMergeShardData(msg, mergeData)
				} else if cleanUp, ok := msg.Command.(CleanShardDataArgs); ok {
					kv.applyCleanUp(msg, cleanUp)
				} else {
					panic("apply err")
				}
			}
		}
	}
}

func (kv *ShardKV) applyOp(msg raft.ApplyMsg, op Op){
	shardId := key2shard(op.Key)
	kv.mu.Lock()
	if kv.configReady(op.ConfigId, op.Key) == OK {
		switch op.Op {
		case "Put":
			if v, ok:=kv.clientLastSeqNum[shardId][op.ClientId];!ok || v < op.SeqNum {
				kv.set(op.Key, op.Value)
				kv.clientLastSeqNum[shardId][op.ClientId] = op.SeqNum
			}
		case "Append":
			if v, ok:=kv.clientLastSeqNum[shardId][op.ClientId];!ok || v < op.SeqNum {
				_, v := kv.get(op.Key)
				kv.set(op.Key,  v + op.Value)
				kv.clientLastSeqNum[shardId][op.ClientId] = op.SeqNum
			}
		case "Get":
		default:
			log.Fatalf("unknown op: %s", op.Op)
		}
		
		if ch, ok := kv.notifyChes[op.NotifyId]; ok {
			_, v := kv.get(op.Key)
			ch <- NotifyMsg{
				Err:   OK,
				Value: v,
			}
		}
		kv.mu.Unlock()

		kv.saveSnapshot(msg.CommandIndex)
	} else{
		if ch, ok := kv.notifyChes[op.NotifyId]; ok {
			go func(){
				t := time.NewTimer(time.Millisecond * 50000)
				defer t.Stop()
				select{
					case ch <- NotifyMsg{Err:ErrWrongGroup} :
					case <-t.C:
				}
			}()
		}
		kv.mu.Unlock()
		kv.saveSnapshot(msg.CommandIndex)
	}
}

func (kv *ShardKV) applyConfig(msg raft.ApplyMsg, config shardctrler.Config) {
	kv.mu.Lock()

	if config.Num <= kv.current.Num {
		kv.mu.Unlock()
		kv.saveSnapshot(msg.CommandIndex)
		return
	}
	if config.Num != kv.current.Num+1 {
		kv.mu.Unlock()
		panic("applyConfig err")
	}


	oldConfig := kv.current.Copy()
	deleteShardIds := make([]int, 0, shardctrler.NShards)
	ownShardIds := make([]int, 0, shardctrler.NShards)
	newShardIds := make([]int, 0, shardctrler.NShards)

	for i := 0; i < shardctrler.NShards; i++ {
		if config.Shards[i] == kv.gid {
			ownShardIds = append(ownShardIds, i)
			if oldConfig.Shards[i] != kv.gid {
				newShardIds = append(newShardIds, i)
			}
		} else {
			if oldConfig.Shards[i] == kv.gid {
				deleteShardIds = append(deleteShardIds, i)
			}
		}
	}

	d := make(map[int]ShardData)
	for _, shardId := range deleteShardIds {
		shardData := ShardData{
			CommonShardArgs: CommonShardArgs{
				ConfigId:  oldConfig.Num,
				ShardId:   shardId,
			},
			Data:       kv.MemoryStorage[shardId],
			ClientLastSeqNum: kv.clientLastSeqNum[shardId],
		}
		d[shardId] = shardData
		kv.MemoryStorage[shardId] = make(map[string]string)
		kv.clientLastSeqNum[shardId] = make(map[int64]int64)
	}
	kv.shardStatus.expire[oldConfig.Num] = d

	kv.shardStatus.own = make(map[int]bool)
	for _, shardId := range ownShardIds {
		kv.shardStatus.own[shardId] = true
	}
	kv.shardStatus.wait = make(map[int]bool)
	if oldConfig.Num != 0 {
		for _, shardId := range newShardIds {
			kv.shardStatus.wait[shardId] = true
		}
	}

	kv.current = config.Copy()
	kv.previous = oldConfig
	kv.mu.Unlock()
	kv.saveSnapshot(msg.CommandIndex)
}

func (kv *ShardKV) applyMergeShardData(msg raft.ApplyMsg, data ShardData) {
	kv.mu.Lock()

	if kv.current.Num != data.ConfigId+1 {
		kv.mu.Unlock()
		kv.saveSnapshot(msg.CommandIndex)
		return
	}
	if _, ok := kv.shardStatus.wait[data.ShardId]; !ok {
		kv.mu.Unlock()
		kv.saveSnapshot(msg.CommandIndex)
		return
	}
	kv.MemoryStorage[data.ShardId] = make(map[string]string)
	kv.clientLastSeqNum[data.ShardId] = make(map[int64]int64)
	for k, v := range data.Data {
		kv.MemoryStorage[data.ShardId][k] = v
	}
	for k, v := range data.ClientLastSeqNum {
		kv.clientLastSeqNum[data.ShardId][k] = v
	}
	delete(kv.shardStatus.wait, data.ShardId)
	go kv.reqCleanShardData(kv.previous, data.ShardId)
	kv.mu.Unlock()
	kv.saveSnapshot(msg.CommandIndex)
}

func (kv *ShardKV) applyCleanUp(msg raft.ApplyMsg, data CleanShardDataArgs) {
	kv.mu.Lock()
	if kv.expireDataExist(data.ConfigId, data.ShardId) {
		delete(kv.expire[data.ConfigId], data.ShardId)
	}
	kv.mu.Unlock()
	kv.saveSnapshot(msg.CommandIndex)
}

func (kv *ShardKV) expireDataExist(configId int, shardId int) bool {
	if _, ok := kv.shardStatus.expire[configId]; ok {
		if _, ok = kv.shardStatus.expire[configId][shardId]; ok {
			return true
		}
	}
	return false
}

func (kv *ShardKV) reqCleanShardData(config shardctrler.Config, shardId int) {
	configId := config.Num
	args := &CleanShardDataArgs{
		CommonShardArgs: CommonShardArgs{
			ConfigId: configId,
			ShardId:  shardId,
		},
	}
	ReqCleanShardDataTimeOut := time.Millisecond * 500
	t := time.NewTimer(ReqCleanShardDataTimeOut)
	defer t.Stop()

	for {
		for _, s := range config.Groups[config.Shards[shardId]] {
			reply := &CleanShardDataReply{}
			srv := kv.make_end(s)
			done := make(chan bool, 1)
			r := false

			go func(args *CleanShardDataArgs, reply *CleanShardDataReply) {
				done <- srv.Call("ShardKV.CleanShardData", args, reply)
			}(args, reply)

			t.Reset(ReqCleanShardDataTimeOut)

			select {
			case <-kv.killCh:
				return
			case r = <-done:
			case <-t.C:

			}
			if r == true && reply.Success == true {
				return
			}
		}
		kv.mu.Lock()
		if kv.current.Num != configId + 1 || len(kv.shardStatus.wait) == 0 {
			kv.mu.Unlock()
			break
		}
		kv.mu.Unlock()
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
	close(kv.killCh)
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}


//
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
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})
	labgob.Register(FetchShardDataArgs{})
	labgob.Register(FetchShardDataReply{})
	labgob.Register(CleanShardDataArgs{})
	labgob.Register(CleanShardDataReply{})
	labgob.Register(ShardData{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.killCh = make(chan struct{})

	kv.MemoryStorage = [shardctrler.NShards]map[string]string{}

	for i, _ := range kv.MemoryStorage {
		kv.MemoryStorage[i] = make(map[string]string)
	}

	kv.clientLastSeqNum = [shardctrler.NShards]map[int64]int64{}
	for i, _ := range kv.clientLastSeqNum {
		kv.clientLastSeqNum[i] = make(map[int64]int64)
	}

	kv.shardStatus.wait = make(map[int]bool)
	kv.shardStatus.expire = make(map[int]map[int]ShardData)
	config := shardctrler.Config{
		Num:    0,
		Shards: [shardctrler.NShards]int{},
		Groups: map[int][]string{},
	}
	kv.current = config
	kv.previous = config

	kv.recover(nil)
	
	kv.notifyChes = make(map[int64]chan NotifyMsg)
	kv.pullConfigTimer = time.NewTimer(PullConfigInterval)
	kv.pullShardDataTimer = time.NewTimer(PullShardDataInterval)

	go kv.applier()
	go kv.pullConfig()
	go kv.pullShardData()

	return kv
}


