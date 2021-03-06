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
	Candidate
	Follower
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
	rol   Rol
	timer *time.Timer
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
	CandidateId  int
	Term         int
	LastLogIndex int
	LastLogTerm  int
	Ch           chan int
	// Your data here.
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here.
}

//
// example RequestEntries RPC arguments structure.
//
type RequestEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []int
	LeaderCommit int
	Ch           chan int
	// Your data here.
}

//
// example RequestEntries RPC reply structure.
//
type RequestEntriesReply struct {
	Term    int
	Success bool
	// Your data here.
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// The candidate's term is greater or equal to servers
	reply.VoteGranted = false

	if args.Term >= rf.currentTerm {

		if args.Term > rf.currentTerm {
			rf.votedFor = -1
		}

		rf.currentTerm = args.Term
		rf.rol = Follower
		DPrintf("%v is follower", rf.me)

		// The candidate's log is at least up-to-date to receiver
		//if args.LastLogIndex >= rf.commitIndex && // Check if it's commitIndex or lastApplied
		// If it has voted previously for this candidate or hasn't vote yet
		if rf.votedFor == args.CandidateId || rf.votedFor == -1 { // Check this value
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		}
	}
	reply.Term = rf.currentTerm
	DPrintf("ReqestVote: %v -> %v\nCurrent role: %v\nReply: %v ", args.CandidateId, rf.me, rf.rol, reply)
}

func (rf *Raft) AppendEntries(args RequestEntriesArgs, reply *RequestEntriesReply) {
	interval := (rand.Intn(ElectionTimeoutHigh-ElectionTimeoutLow) + ElectionTimeoutLow)
	rf.timer.Reset(time.Duration(interval) * time.Millisecond)

	DPrintf("%v got heartbeat cTerm %v Leader %v cTerm %v", rf.me, rf.currentTerm, args.LeaderId, args.Term)

	if rf.rol == Candidate {
		rf.rol = Follower
		DPrintf("%v is follower", rf.me)
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		DPrintf("%v is follower", rf.me)
		rf.rol = Follower
	}
	//DPrintf("Leaving heartbeat with role %v", rf.rol)

}

func (rf *Raft) SendHeartbeat(server int, args RequestEntriesArgs, reply *RequestEntriesReply) bool {
	DPrintf("Envie HB: %v -> %v", rf.me, server)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf("Recibi respuesta de HB: %v <- %v", rf.me, server)
	args.Ch <- 1
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
	DPrintf("Envie UC: %v -> %v", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("Recibi respuesta de UC: %v <- %v", rf.me, server)
	args.Ch <- server
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
	DPrintf("KILL: %v", rf.me)
	// Your code here, if desired.
}

func (rf *Raft) upgradeToCandidate(ch chan int) {
	reply := make([]RequestVoteReply, len(rf.peers))
	voteArgs := RequestVoteArgs{rf.me, rf.currentTerm + 1, rf.commitIndex, rf.currentTerm, make(chan int, len(rf.peers)-1)}
	rf.rol = Candidate
	DPrintf("%v is follower", rf.me)
	rf.currentTerm++
	//execTerm := rf.currentTerm
	votes := 1
	rf.votedFor = rf.me

	DPrintf("Starting a new vote from %v, cTerm: %v", rf.me, rf.currentTerm)
	// TODO: Check if reply is being correctly populated
	for index := range rf.peers {
		if index != rf.me {
			go rf.sendRequestVote(index, voteArgs, &reply[index])
		}
	}

	for index := range rf.peers {
		if index != rf.me {
			server := <-voteArgs.Ch
			if reply[server].VoteGranted {
				votes++
				if votes > len(rf.peers)/2 {
					rf.rol = Leader
					DPrintf("%v is leader", rf.me)
					break
				}
			}
		}
	}

	DPrintf("Out chan check: upgradeToCandidate")

	DPrintf("%v with rol %v obtained %v votes from a total of %v in term %v", rf.me, rf.rol, votes, len(rf.peers), rf.currentTerm)

	ch <- 1
}

func (rf *Raft) heartbeat(ch chan int) {
	reply := make([]RequestEntriesReply, len(rf.peers))
	var entries RequestEntriesArgs
	entries.Term = rf.currentTerm
	entries.LeaderId = rf.me
	entries.Ch = make(chan int, len(rf.peers)-1)

	if rf.rol != Leader {
		DPrintf("From heartbeat %v not the leader anymore", rf.me)
		ch <- 1
		return
	}

	for index := range rf.peers {
		if index != rf.me {
			go rf.SendHeartbeat(index, entries, &reply[index])
		}
	}

	for index := range rf.peers {
		if index != rf.me {
			<-entries.Ch
			DPrintf("Termino RPC: %v <- %v", rf.me, index)
		}
	}
	DPrintf("Out chan check: heartbeat")

	for index := range rf.peers {
		//check if still leader
		if reply[index].Term > rf.currentTerm {
			DPrintf("Leader %v with term %v got %v term from %v", rf.me, rf.currentTerm, reply[index].Term, index)
			rf.rol = Follower
			DPrintf("%v is follower", rf.me)
			rf.currentTerm = reply[index].Term
			ch <- 1
			return
		}
	}
	ch <- 1
}

func (rf *Raft) checkLiveness() {
	ch := make(chan int, 0)

	for {
		DPrintf("CL")
		for rf.rol != Leader {
			interval := (rand.Intn(ElectionTimeoutHigh-ElectionTimeoutLow) + ElectionTimeoutLow)
			timeout := time.Duration(interval) * time.Millisecond
			rf.timer = time.AfterFunc(timeout, func() { rf.upgradeToCandidate(ch) })
			<-ch
			DPrintf("Server %v After waiting for upgrade to Candidate", rf.me)
		}

		// Heartbeats
		for rf.rol == Leader {
			interval := (rand.Intn(HBTimeHigh-HBTimeLow) + HBTimeLow)
			timeout := time.Duration(interval) * time.Millisecond
			DPrintf("HB")
			rf.timer = time.AfterFunc(timeout, func() { rf.heartbeat(ch) })
			<-ch
			DPrintf("Server %v After Sending heartbeats", rf.me)
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
	rf.currentTerm = 0

	rf.rol = Follower
	DPrintf("%v is follower", rf.me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1

	rf.nextIndex = make([]int, 0)
	rf.matchIndex = make([]int, 0)

	go rf.checkLiveness()

	return rf
}
