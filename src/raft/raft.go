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

import "sync"
import "sync/atomic"
import "../labrpc"
import "math/rand"
import "time"

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

const (
	follower  = 0 //initial state when boot
	candidate = 1
	leader    = 2
)

var roleName = [...]string{
	"follower",
	"candidate",
	"leader",
}
type LogEntry struct {
	Command uint32 // what type?
	Term    uint32
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

	role          uint
	lastHeartbeat int64 // last time heard from the leader
	// Persistent state
	currentTerm   int
	votedFor      int
	logs          []LogEntry
	// Volatile state
	commitIndex   uint
	lastApplied   uint
	// Volatile state on leaders
	nextIndex     map[uint]uint
	matchIndex    map[uint]uint
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = rf.role == leader
	rf.mu.Unlock()
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
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

type retEntry struct {
	ok    bool
	reply interface{}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			DPrintf("role changed, me:%d, %s->%s\n", rf.me, roleName[rf.role], roleName[follower])
			rf.role = follower
		}

		rf.lastHeartbeat = time.Now().UnixNano()
		reply.Term = rf.currentTerm
		if rf.votedFor == -1 || rf.votedFor == args.CandidateId { // TODO: add conditions
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
		} else {
			reply.VoteGranted = false
		}
	}
	rf.mu.Unlock()
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			DPrintf("role changed, me:%d, %s->%s\n", rf.me, roleName[rf.role], roleName[follower])
			rf.role = follower
		}

		if rf.role == candidate {
			DPrintf("role changed, me:%d, %s->%s\n", rf.me, roleName[rf.role], roleName[follower])
			rf.role = follower
		}

		rf.lastHeartbeat = time.Now().UnixNano()
		rf.votedFor = args.LeaderId

		reply.Term = rf.currentTerm
		reply.Success = true
	}
	rf.mu.Unlock()
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

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, retChan *chan retEntry, wg *sync.WaitGroup) {
	var reply RequestVoteReply
	DPrintf("RequestVote req, %d->%d, term:%d, candidateid:%d\n",
		rf.me, server, args.Term, args.CandidateId)
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	DPrintf("RequestVote ack, %d->%d, ok:%t, term:%d, votegranted:%t\n",
		server, rf.me, ok, reply.Term, reply.VoteGranted)
	var chanEntry retEntry
	chanEntry.ok = ok
	chanEntry.reply = reply
	*retChan <- chanEntry
	wg.Done()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, retChan *chan retEntry, wg *sync.WaitGroup) {
	var reply AppendEntriesReply
	DPrintf("AppendEntries req, %d->%d, term:%d, leaderid:%d\n",
		rf.me, server, args.Term, args.LeaderId)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, &reply)
	DPrintf("AppendEntries ack, %d->%d, ok:%t, term:%d, success:%t\n",
		server, rf.me, ok, reply.Term, reply.Success)
	var chanEntry retEntry
	chanEntry.ok = ok
	chanEntry.reply = reply
	*retChan <- chanEntry
	wg.Done()
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

func (rf *Raft) election() {
	rf.lastHeartbeat = time.Now().UnixNano()
	for {
		if rf.killed() {
			break
		}

		timeout := int64((rand.Intn(150) + 250) * 1000000) // [250, 400) ms
		time.Sleep(time.Duration(1) * time.Millisecond) // checking every 5 ms

		rf.mu.Lock()
		var interval = time.Now().UnixNano() - rf.lastHeartbeat
		if rf.role != leader && rf.votedFor == -1 && interval >= timeout {
			DPrintf("role changed, me:%d, %s->%s\n", rf.me, roleName[rf.role], roleName[candidate])
			rf.role = candidate
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.lastHeartbeat = time.Now().UnixNano()
			term := rf.currentTerm
			rf.mu.Unlock()

			DPrintf("timeout(%d), election, me:%d, term:%d\n", interval, rf.me, term)
			var args RequestVoteArgs
			args.Term = term
			args.CandidateId = rf.me
			// TODO: initiate LastLogIndex and LastLogTerm

			wg := sync.WaitGroup{}
			retChan := make(chan retEntry)
			for index, _ := range rf.peers {
				if index != rf.me {
					wg.Add(1)
					go rf.sendRequestVote(index, &args, &retChan, &wg)
				}
			}

			go func() {
				wg.Wait()
				close(retChan)
			}()

			rf.mu.Lock()
			if rf.role == candidate && rf.currentTerm == term {
				var voteNumber int
				for entry := range retChan {
					// timeout
					if entry.ok == false {
						continue
					}
					// outdated
					if entry.reply.(RequestVoteReply).Term > rf.currentTerm {
						rf.currentTerm = entry.reply.(RequestVoteReply).Term
						DPrintf("role changed, me:%d, %s->%s\n", rf.me, roleName[rf.role], roleName[follower])
						rf.role = follower
						continue
					}
					// got a vote
					if entry.reply.(RequestVoteReply).VoteGranted == true {
						voteNumber++
					}
				}

				// wins the election
				if rf.role == candidate && voteNumber * 2 > len(rf.peers) {
					DPrintf("role changed, me:%d, %s->%s\n", rf.me, roleName[rf.role], roleName[leader])
					rf.role = leader
					rf.votedFor = rf.me
					DPrintf("win the election, me:%d, votenr:%d\n", rf.me, voteNumber)
				}
			}
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) heartbeat() {
	for {
		if rf.killed() {
			break
		}

		rf.mu.Lock()
		if rf.role == leader {
			DPrintf("send heartbeat, me:%d\n", rf.me)
			term := rf.currentTerm
			rf.mu.Unlock()

			var args AppendEntriesArgs
			args.Term = term
			args.LeaderId = rf.me
			//TODO initiate PrevLogIndex, PrevLogTerm, Entries and LeaderCommit

			wg := sync.WaitGroup{}
			retChan := make(chan retEntry)
			for index, _ := range rf.peers {
				if index != rf.me {
					wg.Add(1)
					go rf.sendAppendEntries(index, &args, &retChan, &wg)
				}
			}

			go func() {
				wg.Wait()
				close(retChan)
			}()

			rf.mu.Lock()
			if rf.role == leader && rf.currentTerm == term {
				for entry := range retChan {
					// timeout
					if entry.ok == false {
						continue
					}
					// outdated
					if entry.reply.(AppendEntriesReply).Term > rf.currentTerm {
						rf.currentTerm = entry.reply.(AppendEntriesReply).Term
						DPrintf("role changed, me:%d, %s->%s\n", rf.me, roleName[rf.role], roleName[follower])
						rf.role = follower
						continue
					}
					// TODO: handle success
				}
			}
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}
		time.Sleep(time.Duration(100) * time.Millisecond)
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

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	go rf.election()
	go rf.heartbeat()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())


	return rf
}
