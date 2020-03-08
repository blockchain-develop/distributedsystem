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
	"log"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
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
	role                           int
	currentTerm                    int
	voteFor                        int
	vote2MeCount                   int
	timer                          *time.Timer
	requestVoteArgsChan            chan *RequestVoteArgs
	requestVoteReplyChan           chan *RequestVoteReply
	requestVoteReplyInternalChan   chan *RequestVoteReply
	appendEntriesArgsChan          chan *AppendEntriesArgs
	appendEntriesReplyChan         chan *AppendEntriesReply
	appendEntriesReplyInternalChan chan *AppendEntriesReply
}

const (
	FOLLOWER   = iota
	CANDIDATE
	LEADER
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.role == LEADER)
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term                int
	LeaderId            int
	PrevLogIndex        int
	PrevLogTerm         int
	Entries             []Entrie
	LeaderCommit        int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term                 int
	Success              bool
}

type Entrie struct {

}

//
// example RequestVote RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.appendEntriesArgsChan <- args
	reply, ok := <- rf.appendEntriesReplyInternalChan
	if !ok || reply == nil {
		log.Fatal("append entries fatal")
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term                int
	CandidateId         int
	LastLogIndex        int
	LastLogTerm         int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term                 int
	VoteGranted          bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.requestVoteArgsChan <- args
	reply, ok := <- rf.requestVoteReplyInternalChan
    if !ok || reply == nil {
    	log.Fatal("Request vote fatal.")
	}
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
func (rf *Raft) startElection() {
	rf.currentTerm ++
	rf.voteFor = rf.me
	rf.vote2MeCount = 1
	for i, _ := range rf.peers {
		if i != rf.me {
			go func() {
				args := &RequestVoteArgs{
					Term: rf.currentTerm,
				}
				reply := RequestVoteReply{}
				rf.sendRequestVote(i, args, &reply)
				rf.requestVoteReplyChan <- &reply
			}()
		}
	}
}

func (rf *Raft) startHeartbeat() {
	for i, _ := range rf.peers {
		if i != rf.me {
			go func() {
				args := &AppendEntriesArgs{
					Term: rf.currentTerm,
				}
				reply := AppendEntriesReply{}
				rf.sendAppendEntries(i, args, &reply)
				rf.appendEntriesReplyChan <- &reply
			}()
		}
	}
}

func (rf *Raft) handleRequestVote(args *RequestVoteArgs) *RequestVoteReply {
	reply := &RequestVoteReply{}
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return reply
	}
	if rf.voteFor == -1 {
		reply.VoteGranted = true
		rf.voteFor = args.CandidateId
		rf.timer.Reset(time.Millisecond * 300)
		return reply
	}
	reply.VoteGranted = false
	return reply
}

func (rf *Raft) handleReqeustVoteReply(reply *RequestVoteReply) {
	if reply.VoteGranted == false {
		return
	}
	if rf.role != CANDIDATE {
		return
	}
	rf.vote2MeCount ++
	if rf.vote2MeCount > len(rf.peers)/2 {
		rf.role = LEADER
		rf.timer.Reset(time.Millisecond * 100)
	}
}

func (rf *Raft) handleAppendEntries(args *AppendEntriesArgs) *AppendEntriesReply {
	reply := &AppendEntriesReply{}
	if args.Term < rf.currentTerm {
		reply.Success = false
		return reply
	}
	reply.Success = true
	rf.currentTerm = args.Term
	rf.voteFor = -1
	rf.role = FOLLOWER
	rf.timer.Reset(time.Millisecond * 300)
	return reply
}

func (rf *Raft) handleAppendEntriesReply(reply *AppendEntriesReply) {

}

func (rf *Raft) eventLoop() {
	for {
		select {
		case <- rf.timer.C:
			if rf.role == FOLLOWER || rf.role == CANDIDATE {
				rf.role = CANDIDATE
				rf.startElection()
				rf.timer.Reset(time.Millisecond * 300)
			} else {
				rf.startHeartbeat()
				rf.timer.Reset(time.Millisecond * 100)
			}
		case requestVoteArgs, ok :=  <- rf.requestVoteArgsChan:
			if !ok || requestVoteArgs == nil {
				break
			}
			reply := rf.handleRequestVote(requestVoteArgs)
			rf.requestVoteReplyInternalChan <- reply
		case requestVoteReply, ok := <- rf.requestVoteReplyChan:
			if !ok || requestVoteReply == nil {
				break
			}
			rf.handleReqeustVoteReply(requestVoteReply)
		case appendEntriesArgs, ok := <- rf.appendEntriesArgsChan:
			if !ok || appendEntriesArgs == nil {
				break
			}
			reply := rf.handleAppendEntries(appendEntriesArgs)
			rf.appendEntriesReplyInternalChan <- reply
		case appendEntriesReply, ok := <- rf.appendEntriesReplyChan:
			if !ok || appendEntriesReply == nil {
				break
			}
			rf.handleAppendEntriesReply(appendEntriesReply)
		}
	}
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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

	// Your initialization code here (2A, 2B, 2C).
	rf.role = FOLLOWER
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.vote2MeCount = 0

	rf.requestVoteArgsChan = make(chan *RequestVoteArgs)
	rf.requestVoteReplyChan = make(chan *RequestVoteReply)
	rf.requestVoteReplyInternalChan = make(chan *RequestVoteReply)
	rf.appendEntriesArgsChan = make(chan *AppendEntriesArgs)
	rf.appendEntriesReplyChan = make(chan *AppendEntriesReply)
	rf.appendEntriesReplyInternalChan = make(chan *AppendEntriesReply)
	rf.timer = time.NewTimer(time.Millisecond * 300)
	go rf.eventLoop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
