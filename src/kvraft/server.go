package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

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
	SeqNum int64
	ClientId int64
	Key string
	Value string
	Method string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.

	killCh  chan struct{}
	MemoryStorage
	persister *raft.Persister

	notifyChes map[int64]chan NotifyMsg

	clientLastSeqNum map[int64]int64

	lastApplyIndex int
	lastApplyTerm  int
}

type MemoryStorage map[string]string

func (ms MemoryStorage) get(key string)(err Err, value string){
	if v, ok := ms[key]; ok {
		err = OK
		value = v
		return
	} else {
		err = ErrNoKey
		return
	}
}

func (ms MemoryStorage) set(key,value string){
	ms[key] = value
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		SeqNum:    args.SeqNum,
		Key:      args.Key,
		Method:   "Get",
		ClientId: args.ClientId,
	}
	res := kv.start(op)
	reply.Err = res.Err
	reply.Value = res.Value
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	op := Op{
		SeqNum:    args.SeqNum,
		Key:      args.Key,
		Value:    args.Value,
		Method:   args.Op,
		ClientId: args.ClientId,
	}
	reply.Err = kv.start(op).Err
}

func (kv *KVServer) applier(){
	for {
		select {
		case <-kv.killCh:
			return
		case msg := <-kv.applyCh:
			if msg.CommandValid{
				op := msg.Command.(Op)

				kv.mu.Lock()

				switch op.Method {
				case "Put":
					if v, ok:=kv.clientLastSeqNum[op.ClientId];!ok || v < op.SeqNum {
						kv.set(op.Key, op.Value)
						kv.clientLastSeqNum[op.ClientId] = op.SeqNum
					}
				case "Append":
					if v, ok:=kv.clientLastSeqNum[op.ClientId];!ok || v < op.SeqNum {
						_, v := kv.get(op.Key)
						kv.set(op.Key,  v + op.Value)
						kv.clientLastSeqNum[op.ClientId] = op.SeqNum
					}
				case "Get":
					kv.clientLastSeqNum[op.ClientId] = op.SeqNum
				default:
					log.Fatalf("unknown method: %s", op.Method)
				}
				
				if ch, ok := kv.notifyChes[op.SeqNum]; ok {
					_, v := kv.get(op.Key)
					go func(){
						t := time.NewTimer(time.Millisecond * 50000)
						defer t.Stop()
						select{
							case ch <- NotifyMsg{
								Err:   OK,
								Value: v,
							} :
							case <-t.C:
						}
					}()
				}
				kv.mu.Unlock()

				if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate * 9 / 10 {
					data := kv.encodeKVData()
					kv.rf.Snapshot(msg.CommandIndex, data)
				}
			} else {
				if msg.SnapshotValid {
					kv.mu.Lock()
					kv.recover(msg.Snapshot)
					kv.mu.Unlock()
				}
			}
		}
	}
}

func (kv *KVServer) start(op Op) (res NotifyMsg){
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		res.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := make(chan NotifyMsg, 1)
	kv.notifyChes[op.SeqNum] = ch
	kv.mu.Unlock()

	t := time.NewTimer(time.Millisecond * 300)
	defer t.Stop()
	select {
	case res = <-ch:
		kv.mu.Lock()
		delete(kv.notifyChes,op.SeqNum)
		kv.mu.Unlock()
		return
	case <-t.C:
		kv.mu.Lock()
		delete(kv.notifyChes,op.SeqNum)
		kv.mu.Unlock()
		res.Err = ErrTimeOut
		return
	}
}

func (kv *KVServer) recover(data []byte){
	if data == nil || len(data) < 1 { 
		data = kv.persister.ReadSnapshot()
	}

	if data == nil || len(data) < 1 { 
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var kvData map[string]string 
	var clientLastSeqNum map[int64]int64

	if d.Decode(&kvData) != nil ||  d.Decode(&clientLastSeqNum) != nil{
		log.Fatal("kv recover err")
	} else {
		kv.MemoryStorage = kvData
		kv.clientLastSeqNum = clientLastSeqNum
	}
}

func (kv *KVServer) encodeKVData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.MemoryStorage); err != nil {
		panic(err)
	}
	if err := e.Encode(kv.clientLastSeqNum); err != nil {
		panic(err)
	}
	data := w.Bytes()
	return data
}


//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.killCh)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
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
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.persister = persister

	kv.MemoryStorage = make(map[string]string)
	kv.clientLastSeqNum = make(map[int64]int64)
	kv.killCh = make(chan struct{})
	kv.notifyChes = make(map[int64]chan NotifyMsg)
	
	kv.recover(nil)

	go kv.applier()

	return kv
}
