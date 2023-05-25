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

	logstd "log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

type Command struct {
	Tofill int
}

// struct of log entry
type LogEntry struct {
	Command interface{}
	Term    int
}

const (
	Follower = iota
	Candidate
	Leader
)

type ServerStatus = int

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	electionTimeout time.Time // if time elapsed more than this, start election
	eleapsedTime    int64     // how much time elapsed from last heartbeat
	heartbeatTime   int64

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm int // initalized to 0
	votedFor    int // andidatedId that received vote in current term, -1 for null
	log         []LogEntry

	// volatile states
	commitIndex int // index of highest log entry know to be committed
	lastApplied int // index of highest log entry applied to state machine

	// volatile states on leaders
	nextIndex  []int
	matchIndex []int

	status ServerStatus

	applyChan chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.status == Leader
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// func (arg *RequestVoteArgs) String() string {
// 	return fmt.Sprintf("RequestVoteArgs{Term: %d, CandidateId: %d, LastLogIndex: %d, LastLogTerm: %d, }",
// 		arg.Term, arg.CandidateId, arg.LastLogIndex, arg.LastLogTerm)
// }

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // for candidate to update self
	VoteGranted bool // true means candidate reveived vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	logstd.Printf("#### peer %d receive requestVote: args: %+v\n", rf.me, args)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		logstd.Printf("args.Term < currentTerm\n")
		return
	}
	if args.Term > rf.currentTerm {
		rf.SetCurrentTerm(args.Term)
	}
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if rf.moreUpTodate(args.LastLogIndex, args.LastLogTerm) {
			reply.VoteGranted = true
			return
		}
	}
	reply.VoteGranted = false
}

func (rf *Raft) moreUpTodate(lastLogIndex int, lastLogTerm int) bool {
	if len(rf.log) <= 0 {
		return true
	}
	if lastLogIndex < len(rf.log) || lastLogTerm < rf.log[len(rf.log)-1].Term {
		logstd.Printf("peer %d: lastLogIndex: %d, len of log: %d, lastLogTerm: %d, log[last].term: %d\n",
			rf.me, lastLogIndex, len(rf.log), lastLogTerm, rf.log[len(rf.log)-1])
		return false
	}
	return true

}

type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int
	PrevLogIndex int // index of log entry immediatedly preceding new ones

	PrevLogTerm int
	Entries     []LogEntry // log entries to store(empty for heartbeat)

	LeaderCommit int // leader's commit Index
}

type AppendEntriesReply struct {
	Term    int  //for leader to update
	Success bool // if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	logstd.Printf("#### peer %d receive AppendEntries: %+v", rf.me, args)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	// 2
	if args.PrevLogIndex != -1 && len(rf.log) >= args.PrevLogIndex && rf.log[args.PrevLogIndex].Term == args.PrevLogTerm {
		logstd.Printf("#### peer %d log not match: %+v", rf.me, rf.log)
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}

	rf.resetElectionTimeout()
	if args.Term > rf.currentTerm {
		rf.SetCurrentTerm(args.Term)
	}

	// 3,4
	for i, entry := range args.Entries {
		if i+args.PrevLogIndex+1 >= len(rf.log) {
			rf.log = append(rf.log, entry)
		} else {
			rf.log[i+args.PrevLogIndex+1] = entry
		}
	}

	// 5
	if args.LeaderCommit > rf.commitIndex {
		if args.LeaderCommit < len(rf.log)-1 {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = len(rf.log) - 1
		}
	}

	rf.mu.Unlock()

	reply.Success = true

}

// lock before this
func (rf *Raft) ApplyLog() {
	for rf.commitIndex > rf.lastApplied {
		rf.ApplyLog()
	}
}

// lock before this
func (rf *Raft) SetCurrentTerm(term int) {
	rf.currentTerm = term
	rf.status = Follower
	rf.votedFor = -1
}

// will lock in body
func (rf *Raft) sendAppendEntries(ifHeartBeat bool) {
	if ifHeartBeat {
		logstd.Printf("---- peer %d heartbeat", rf.me)
		rf.mu.Lock()
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: -1,
		}
		rf.mu.Unlock()
		for peerId, _ := range rf.peers {
			if peerId != rf.me {
				logstd.Printf("---- peer %d send AppenEntries to peer %d", rf.me, peerId)
				go rf.sendAppendEntry(peerId, args)
			}
		}
	} else {
		logstd.Printf("---- peer %d send AppenEntries", rf.me)
		rf.mu.Lock()
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: len(rf.log) - 2,
			PrevLogTerm:  rf.log[len(rf.log)-2].Term,
			Entries:      rf.log[len(rf.log)-1:],
		}
		rf.mu.Unlock()
		for peerId, _ := range rf.peers {
			if peerId != rf.me {
				logstd.Printf("---- peer %d send AppenEntries to peer %d", rf.me, peerId)
				go rf.sendAppendEntry(peerId, args)
			}
		}
	}
}

func (rf *Raft) sendAppendEntry(peerId int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{Success: false, Term: 0}
	for !rf.killed() && !reply.Success {
		ok := rf.peers[peerId].Call("Raft.AppendEntries", args, reply)
		if !ok {
			logstd.Printf("%d to %d: call AppendEntries fail!!!!", rf.me, peerId)
			return
		}
		logstd.Printf("!!!! peer %d getback AppenEntries result: %+v", rf.me, reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !reply.Success {
			if reply.Term > rf.currentTerm {
				rf.SetCurrentTerm(reply.Term)
			} else {
				args.PrevLogIndex -= 1
				args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
				args.Entries = rf.log[args.PrevLogIndex+1:]
			}
		}
	}

}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) StartElection() {
	logstd.Printf("-----peer %d start a election\n", rf.me)
	rf.mu.Lock()
	rf.currentTerm++
	term := rf.currentTerm
	rf.status = Candidate // become Candidata
	rf.votedFor = rf.me
	rf.resetElectionTimeout()

	voteCounter := 1 // already vote me
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log) - 1,
		LastLogTerm:  0,
	}
	if len(rf.log) > 0 {
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
	}
	rf.mu.Unlock()

	var becomeLeader sync.Once
	for peerId, _ := range rf.peers {
		if peerId != rf.me {
			go rf.requestCandidate(peerId, &args, &becomeLeader, &voteCounter)
		}
	}
}

func (rf *Raft) requestCandidate(peerId int, args *RequestVoteArgs, becomeLeader *sync.Once, voteCounter *int) {
	reply := RequestVoteReply{}
	ok := rf.peers[peerId].Call("Raft.RequestVote", args, &reply)
	if !ok {
		return
	}
	logstd.Printf("---- peer %d receive reply: %+v", rf.me, reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		rf.SetCurrentTerm(reply.Term)
		return
	}
	if reply.VoteGranted {
		*voteCounter++
		if *voteCounter > len(rf.peers)/2 && args.Term == rf.currentTerm && rf.status == Candidate {
			becomeLeader.Do(func() {
				rf.status = Leader
				rf.resetElectionTimeout()
				logstd.Printf("peer %d become a leader", rf.me)
				// init leader state
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				for i := 0; i < len(rf.peers); i += 1 {
					rf.nextIndex[i] = len(rf.log)
					rf.matchIndex[i] = 0
				}

				rf.mu.Unlock()
				rf.sendAppendEntries(true)
				rf.mu.Lock()
			})
		}
	}
}

// need lock before call this
func (rf *Raft) resetElectionTimeout() {
	rf.electionTimeout = time.Now().Add(time.Duration(400+rand.Int63()%150) * time.Millisecond)
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	index := len(rf.log)
	term := rf.currentTerm
	isLeader := rf.status == Leader

	if isLeader {
		rf.log = append(rf.log, LogEntry{Command: command, Term: term})
	}
	rf.mu.Unlock()

	// Your code here (2B).
	if isLeader {
		go rf.sendAppendEntries(false)
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// fmt.Printf("peer %d killed, dead: %d", rf.me, rf.dead)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// fmt.Printf("peer %d tick:\n", rf.me)
		// Your code here (2A)
		// Check if a leader election should be started.
		// time.Sleep(time.Duration(rf.heartbeatTime) * time.Millisecond)
		if rf.status == Leader {
			rf.sendAppendEntries(true)
		} else {
			if time.Now().After(rf.electionTimeout) {
				rf.StartElection()
			}
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 100 + (rand.Int63() % 50)
		rf.eleapsedTime += ms
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.status = Follower
	rf.resetElectionTimeout()
	rf.eleapsedTime = 0
	rf.heartbeatTime = 100
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.votedFor = -1
	rf.lastApplied = 0
	rf.log = make([]LogEntry, 0, 50)
	_ = append(rf.log)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
