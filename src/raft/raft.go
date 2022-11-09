package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PervLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term      int
	Success   bool

	LogTerm int
	LogIndex int

	Abandon bool
}


//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int
	votedFor int
	log []LogEntry

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int 

	role Role

	applyCh chan ApplyMsg

	electionTimer *time.Timer
	heartbeatTimer *time.Timer

	killCh chan struct{}
	notifyApplyCh chan struct{}
}

type LogEntry struct{
	Term int
	Command      interface{}
}

type Role int

const(
	Follower Role = iota
	Candidate
	Leader
)

const(
	ElectionTimeout = time.Millisecond * 150
	HeartBeatTimeout = time.Millisecond * 100
)

func init() {
	rand.Seed(time.Now().Unix())
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == Leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm{
		return
	} else if args.Term == rf.currentTerm {
		if rf.role == Leader{
			return
		} else if rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			return
		} else if rf.votedFor != -1 && rf.votedFor != args.CandidateId{
			return 
		}
	} else {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.changeToFollower()
	}

	if lastLogTerm > args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex > args.LastLogIndex){
		return 
	}

	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateId 
	rf.changeToFollower()	
	rf.resetElectionTimer()

	reply.VoteGranted = true
	return 
}

func (rf *Raft) lastLogTermIndex() (int,int){
	return rf.log[len(rf.log)-1].Term, len(rf.log) - 1
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = rf.role == Leader
	_, lastIndex := rf.lastLogTermIndex()
	index = lastIndex + 1


	if isLeader {
		rf.log = append(rf.log, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})
		rf.matchIndex[rf.me] = index
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	close(rf.killCh)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	// }
	for {  
		select{
		case <- rf.killCh:
			return
		case <- rf.electionTimer.C:
			go rf.election()
		}
	}
}

func (rf *Raft) election(){
	rf.mu.Lock()
	
	if rf.role == Leader{
		rf.mu.Unlock()
		return
	}

	rf.resetElectionTimer()

	rf.changeToCandidate()


	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	rf.mu.Unlock()

	grantedCount := 1
	votedCount := 1

	votesCh := make(chan bool, len(rf.peers))

	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go func(ch chan bool, index int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(index, &args, &reply)
			ch <- reply.VoteGranted
			if reply.Term > args.Term {
				rf.mu.Lock()
				if rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.changeToFollower()
					rf.votedFor = -1
					rf.resetElectionTimer()
				}
				rf.mu.Unlock()
			}
		}(votesCh, index)
	}

	for {
		r := <-votesCh
		votedCount += 1
		if r == true {
			grantedCount += 1
		}
		if votedCount == len(rf.peers) || grantedCount > len(rf.peers)/2 || votedCount - grantedCount > len(rf.peers)/2 {
			break
		}
	}

	if grantedCount <= len(rf.peers)/2 {
		return
	}

	rf.mu.Lock()
	if rf.currentTerm == args.Term && rf.role == Candidate {
		rf.changeToLeader()
	}

	rf.mu.Unlock()

}

func (rf *Raft) heartbeatTicker(){
	for {
		select {
		case <- rf.killCh:
			return
		case <- rf.heartbeatTimer.C:
			rf.resetHeartbeatTimer()
			for index := range rf.peers {	
				if rf.me == index{
					continue
				} else {
					go rf.heartbeat(index)
				}
			}
		}
	}
	
}

func (rf *Raft) heartbeat(server int){
	rf.mu.Lock()
	if rf.role != Leader{
		rf.mu.Unlock()
		return
	}
	nextIndex := rf.nextIndex[server]
	term:= rf.currentTerm
	rf.mu.Unlock() 

	for !rf.killed(){
		rf.mu.Lock()
		if rf.role != Leader  || rf.currentTerm != term{
			rf.mu.Unlock()
			return
		}
		args := AppendEntriesArgs{
			Term: rf.currentTerm,
			LeaderId: rf.me,
			LeaderCommit: rf.commitIndex,
			Entries: rf.log[nextIndex:],
			PervLogTerm: rf.log[nextIndex-1].Term,
			PrevLogIndex: nextIndex - 1,
		}
		reply := AppendEntriesReply{}
		rf.mu.Unlock()

		rf.sendAppendEntries(server, &args, &reply)

		if reply.Abandon {
			return
		}

		if reply.Term > term {
			rf.mu.Lock()
			if rf.currentTerm < reply.Term {
				rf.currentTerm = reply.Term
				rf.changeToFollower()
				rf.votedFor = -1
				rf.resetElectionTimer()
			}
			rf.mu.Unlock()
			return
		} else {
			rf.mu.Lock()
			if rf.role != Leader || rf.currentTerm != term {
				rf.mu.Unlock()
				return
			}
			if reply.Success {

				rf.nextIndex[server] = nextIndex + len(args.Entries)

				rf.matchIndex[server] = args.PrevLogIndex +  len(args.Entries)

				if len(args.Entries) > 0 && args.Entries[len(args.Entries)-1].Term == rf.currentTerm {
					rf.updateCommitIndex()
				}

				rf.mu.Unlock()
				return
			} else {
				if rf.log[reply.LogIndex].Term == reply.LogTerm {
					nextIndex = reply.LogIndex + 1
				} else {
					nextIndex = reply.LogIndex
				}
			}
			rf.mu.Unlock()
			continue
		}
	}
}

func (rf *Raft) updateCommitIndex() {
	hasCommit := false
	for i := rf.commitIndex + 1; i <= len(rf.log); i++ {
		count := 0
		for _, m := range rf.matchIndex {
			if m >= i {
				count += 1
				if count > len(rf.peers)/2 {
					rf.commitIndex = i
					hasCommit = true
					break
				}
			}
		}
		if rf.commitIndex != i {
			break
		}
	}
	if hasCommit {
		rf.notifyApplyCh <- struct{}{}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLogTerm, lastLogIndex := rf.lastLogTermIndex()

	reply.Success = true
	reply.Term = rf.currentTerm
	reply.LogIndex = lastLogIndex
	reply.LogTerm = lastLogTerm
	reply.Abandon = false
	rf.resetElectionTimer()

	if rf.currentTerm < args.Term{
		rf.changeToFollower()
		rf.votedFor = -1
		rf.currentTerm = args.Term
	} else if rf.currentTerm > args.Term{
		reply.Success = false
		return
	}

	if len(args.Entries) > 0 && (lastLogTerm > args.Entries[len(args.Entries)-1].Term  || (lastLogTerm == args.Entries[len(args.Entries)-1].Term  && lastLogIndex > args.PrevLogIndex + len(args.Entries))){
		reply.Abandon = true
		return 
	}


	if args.PrevLogIndex > lastLogIndex {
		reply.Success = false
		reply.LogIndex = lastLogIndex
		reply.LogTerm = lastLogTerm
	} else {
		 if rf.log[args.PrevLogIndex].Term == args.PervLogTerm{
			rf.log = append(rf.log[0:args.PrevLogIndex+1], args.Entries...)
		 } else {
			reply.Success = false
			idx := args.PrevLogIndex
			for idx > rf.commitIndex && rf.log[idx].Term == rf.log[args.PrevLogIndex].Term {
				idx -= 1
			}
			reply.LogIndex = idx
			reply.LogTerm = rf.log[idx].Term
		 }
	}

	if reply.Success {
		if rf.commitIndex < args.LeaderCommit {
			if args.LeaderCommit > len(rf.log){
				rf.commitIndex = len(rf.log)
			} else {
				rf.commitIndex = args.LeaderCommit
			}
			rf.notifyApplyCh <- struct{}{}
		}
	}
}

func (rf *Raft) getLogByIndex(index int) LogEntry{
	return rf.log[index]
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func randomizeElectionTimeout() time.Duration{
	return ElectionTimeout + (time.Duration(rand.Int63()) % ElectionTimeout)
}

func (rf *Raft) resetElectionTimer(){
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(randomizeElectionTimeout())
}

func (rf *Raft) resetHeartbeatTimer(){
	rf.heartbeatTimer.Stop()
	rf.heartbeatTimer.Reset(HeartBeatTimeout)
}

func (rf *Raft) changeToLeader(){
	rf.role = Leader

	_, lastLogIndex := rf.lastLogTermIndex()
	rf.nextIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = lastLogIndex + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.matchIndex[rf.me] = lastLogIndex
}

func (rf *Raft) changeToCandidate(){
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.currentTerm += 1
}

func (rf *Raft) changeToFollower(){
	rf.role = Follower
}


func (rf *Raft) applyLog(){
	for {
		select {
		case <-rf.killCh:
			return
		case <-rf.notifyApplyCh:
			if rf.lastApplied < rf.commitIndex{
				rf.mu.Lock()
				msgs := make([]ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					msgs = append(msgs, ApplyMsg{
						CommandValid: true,
						Command:      rf.log[i].Command,
						CommandIndex: i,
					})
				}
				for _, msg := range msgs {
					rf.applyCh <- msg
					
					rf.lastApplied = msg.CommandIndex
				}
				rf.mu.Unlock()
			}
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.killCh = make(chan struct{})
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	rf.changeToFollower()

	rf.readPersist(persister.ReadRaftState())

	rf.electionTimer = time.NewTimer(randomizeElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(HeartBeatTimeout)

	rf.notifyApplyCh = make(chan struct{}, 100)

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.heartbeatTicker()

	go rf.applyLog()

	return rf
}
