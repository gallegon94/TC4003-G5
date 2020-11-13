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
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Rol int

const (
	Leader Rol = iota
	Follower
	Candidate
)

const (
	HBTimeLow           int = 8
	HBTimeHigh          int = 16
	ElectionTimeoutLow  int = 150
	ElectionTimeoutHigh int = 300
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]
	// Raft Rol (Leader, Follower, Candidate)
	rol     Rol
	timer   *time.Timer
	channel chan int
	// Persistent state
	currentTerm int
	votedFor    int
	logEntries  map[int]int
	//Volatile state
	commitIndex int
	lastApplied int
	//Volatile state on leaders
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = (rf.rol == Leader)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.logEntries)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	candidateId  int
	term         int
	lastLogIndex int
	lastLogTerm  int
	// Your data here.
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	term        int
	voteGranted bool
	// Your data here.
}

//
// example RequestEntries RPC arguments structure.
//
type RequestEntriesArgs struct {
	term         int
	leaderId     int
	prevLogIndex int
	prevLogTerm  int
	entries      []int
	leaderCommit int
	// Your data here.
}

//
// example RequestEntries RPC reply structure.
//
type RequestEntriesReply struct {
	term    int
	success bool
	// Your data here.
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// The candidate's term is greater or equal to servers
	if args.term >= rf.currentTerm &&
		// The candidate's log is at least up-to-date to receiver
		args.lastLogIndex >= rf.commitIndex && // Check if it's commitIndex or lastApplied
		// If it has voted previously for this candidate or hasn't vote yet
		(rf.votedFor == args.candidateId || rf.votedFor == 0) { // Check this value
		rf.votedFor = args.candidateId
		reply.voteGranted = true
		reply.term = rf.currentTerm
	} else {
		reply.term = rf.currentTerm
		reply.voteGranted = false
	}
}

func (rf *Raft) AppendEntries(args RequestEntriesArgs, reply *RequestEntriesReply) {

	interval := (rand.Intn(ElectionTimeoutHigh-ElectionTimeoutLow) + ElectionTimeoutLow)
	rf.timer.Reset(time.Duration(interval) * time.Millisecond)
	//TODO: Check when to downgrade to Follower
}

func (rf *Raft) sendHeartbeat(server int, args RequestEntriesArgs, reply *RequestEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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

	index = rf.commitIndex + 1
	term = rf.currentTerm
	isLeader = (rf.rol == Leader)

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) upgradeToCandidate() {

	var reply RequestVoteReply
	voteArgs := RequestVoteArgs{rf.me, rf.currentTerm + 1, rf.commitIndex, rf.currentTerm}
	rf.rol = Candidate
	rf.currentTerm += 1
	votes := 1
	rf.votedFor = rf.me

	// TODO: Check if reply is being correctly populated
	for index, _ := range rf.peers {
		if index != rf.me {
			rf.sendRequestVote(index, voteArgs, &reply)

			if reply.voteGranted {
				votes += 1
			}
		}

	}

	if votes > len(rf.peers)/2 {
		rf.rol = Leader
	}

	rf.channel <- 1
}

func (rf *Raft) heartbeat() {
	var reply RequestEntriesReply
	var entries RequestEntriesArgs
	followers := 0
	for index, _ := range rf.peers {
		if index != rf.me {
			rf.sendHeartbeat(index, entries, &reply)

			if reply.success == true {
				followers++
			}
		}

	}

	//TODO	if()

}

func (rf *Raft) checkLiveness() {
	interval := (rand.Intn(ElectionTimeoutHigh-ElectionTimeoutLow) + ElectionTimeoutLow)
	timeout := time.Duration(interval) * time.Millisecond
	var a int

	for rf.rol != Leader {
		rf.timer = time.AfterFunc(timeout, func() { rf.upgradeToCandidate() })
		a = <-rf.channel
	}

	// Heartbeats
	for rf.rol == Leader {
		interval := (rand.Intn(HBTimeHigh-HBTimeLow) + HBTimeLow)
		timeout := time.Duration(interval) * time.Millisecond
		rf.timer = time.AfterFunc(timeout, func() { rf.heartbeat() })
		a = <-rf.channel
	}
	a++
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

	rf.rol = Follower

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)

	go rf.checkLiveness()

	return rf
}
