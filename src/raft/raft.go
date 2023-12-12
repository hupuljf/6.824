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
	"6.824/labgob"
	"bytes"
	"math/rand"
	"sort"

	//	"bytes"
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
	CommandTerm  int //该log的term,以便上层应用可以根据term判断是否过期(index处期望的term和该term不同的情况)

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type AppendEntryArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int //每个follower都知道哪个是leader
	PrevLogIndex int //leader认为follower已经匹配的日志下标 上一个日志下标
	PrevLogTerm  int //leader认为的要添加entry的 上一个term
	Entries      []LogEntry
	LeaderCommit int //用于通知follower更新自己的commit index
}

type AppendEntryReply struct {
	// Your data here (2A).
	Term    int
	Success bool

	// fast back up
	XTerm  int //当前的term
	XIndex int //当前的term的最早的index
	XLen   int // ？
	//XLen: 当前follower所拥有的的日志长度。
	//XTerm: 当前follower的日志中，PrevLogIndex所对应日志项的term。可能为空。
	//XIndex: 当前follower的日志中，拥有XTerm的日志项的最低index，可能为空。
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//fmt.Println("我被append了", rf.me)
	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Success = false
		return
	}

	// If RPC request or response contains term T > currentTerm:
	// set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.term || rf.state == candidate { //我收到了你的任期比较大 我把任期改成你 或者咱们任期一样 但是我以为没有leader准备作为候选人去竞争的时候 我先改成follower
		rf.ChangeState(follower, args.Term)

	}

	if args.PrevLogIndex-rf.lastIncludeIndex >= len(rf.log) || (args.PrevLogIndex-rf.lastIncludeIndex >= 1 && rf.log[args.PrevLogIndex-rf.lastIncludeIndex].Term != args.PrevLogTerm) {
		reply.Term = rf.term
		reply.Success = false
		//	fmt.Println("append entry 返回false", args.PrevLogIndex, len(rf.log), rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		reply.XLen = len(rf.log)
		if args.PrevLogIndex-rf.lastIncludeIndex >= 1 && args.PrevLogIndex-rf.lastIncludeIndex < len(rf.log) {
			reply.XTerm = rf.log[args.PrevLogIndex-rf.lastIncludeIndex].Term
			for i := args.PrevLogIndex - rf.lastIncludeIndex; i >= 1; i-- {
				if rf.log[i].Term == reply.XTerm {
					reply.XIndex = i + rf.lastIncludeIndex
				} else {
					break
				}
			}
		}
		rf.electBeginTime = time.Now().Unix()

		return
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it (§5.3)
	//日志覆盖
	misMatchIndex := -1
	for i := range args.Entries {
		if args.PrevLogIndex+1+i-rf.lastIncludeIndex >= len(rf.log) || (args.PrevLogIndex+1+i-rf.lastIncludeIndex >= 0 && rf.log[args.PrevLogIndex+1+i-rf.lastIncludeIndex].Term != args.Entries[i].Term) { //term相同index相同的日志项是可以不覆盖的 其实覆盖也行
			misMatchIndex = i //实际的日志索引
			break
		}
	}
	// Append any new entries not already in the log
	if misMatchIndex != -1 {
		rf.log = append([]LogEntry{}, rf.log[:args.PrevLogIndex+1+misMatchIndex-rf.lastIncludeIndex]...) //得删除日志 得深拷贝 https://blog.csdn.net/freedom1523646952/article/details/127989545
		rf.log = append(rf.log, args.Entries[misMatchIndex:]...)
		rf.persist()
	}
	//if args.PrevLogIndex >= -1 { //强行覆盖
	//	rf.log = rf.log[:args.PrevLogIndex+1]
	//	rf.log = append(rf.log, args.Entries...)
	//}

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	//fmt.Println("小弟的 commit", rf.commitIndex)
	if args.LeaderCommit > rf.commitIndex {
		newEntryIndex := len(rf.log) - 1 + rf.lastIncludeIndex
		if args.LeaderCommit >= newEntryIndex {
			rf.commitIndex = newEntryIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		//fmt.Println("f commitIndex", rf.commitIndex)
		//DPrintf("[%d]: commit index [%d]", rf.me, rf.commitIndex)
		rf.sendApplyMsg()
		//go rf.appMsgApplier()
	}

	rf.electBeginTime = time.Now().Unix()
	reply.Term = rf.term
	reply.Success = true
	//fmt.Println("此时该节点的日志大小：", rf.me, ":", len(rf.log))
	return
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply) //执行rpc的是follower
	return ok
}

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

	//2A
	term int
	//	isLeader       bool
	electBeginTime int64 //选举计时的开始时间 有可能是上一次获取rpc调用的时间/state转换的时间节点
	//electTimeOut time.Duration //选举超时时间 本身一直在更新 没有必要
	votedFor int //主要是用来表明是不是给别的节点投过票了 因为投票规则是先到先得 初始化为-1，如果不是其他节点id就代表可以为当前的请求投
	state    string
	wg       sync.WaitGroup //投票的wg

	log         []LogEntry //2B中还没有用快照 指的就是所有的command序列 各个raft节点可能会不一致
	commitIndex int        //log的下标 该下标以前的log表示当前已经提交的log leader给follower发 提醒他们该提交到这个地方了
	lastApplied int        //log的下标 上个应用到服务（复制状态机）的index 提交指的是当前term超过大多数节点 （提交完以后给他apply不好吗 apply的时机是啥）
	nextIndex   []int      //乐观的估计 leader告诉follower 我下次要给你的log下标 一开始都是leader日志的最后一项
	matchIndex  []int      //悲观的估计 一开始是0 （为什么要有这个东西）---- 为了数据安全 统计大多数节点已经匹配的日志项来得到commitIndex 而nextIndex的话不一定是已经匹配了的index

	applyCh   chan ApplyMsg
	moreApply bool
	applyCond *sync.Cond

	lastIncludeIndex int
	lastIncludeTerm  int //lastIncludeIndex 对应的term
	snapshot         []byte
	//stateLock sync.Mutex //专门给state加的锁 因为state
}

//给状态机（上层应用） 应用已提交的日志
func (rf *Raft) sendApplyMsg() { //控制信号
	rf.moreApply = true
	rf.applyCond.Broadcast()

}

func (rf *Raft) appMsgApplier() {
	for {
		rf.mu.Lock()
		for !rf.moreApply {
			//time.Sleep(10 * time.Millisecond)
			rf.applyCond.Wait()
		}
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		entries := rf.log
		lastIncludeIndex := rf.lastIncludeIndex
		rf.moreApply = false
		rf.mu.Unlock()
		for i := lastApplied + 1; i <= commitIndex; i++ {
			msg := ApplyMsg{true, entries[i-lastIncludeIndex].Command, i, entries[i-lastIncludeIndex].Term, false, []byte{}, -1, -1} //ggle
			//DPrintf("[%d]: apply index %d - 1", rf.me, msg.CommandIndex)
			rf.applyCh <- msg
			//fmt.Println("此时apply了", rf.state, rf.commitIndex)
			rf.mu.Lock()
			rf.lastApplied = i
			rf.mu.Unlock()
		}

	} //
}

type LogEntry struct {
	Command interface{}
	Term    int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	//var isleader bool
	// Your code here (2A).
	rf.mu.Lock() //加锁 防止中间状态的变量误读
	defer rf.mu.Unlock()

	term = rf.term
	isLeader := rf.state == leader
	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	//	e.Encode(rf.lastApplied)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var log []LogEntry
	var lastIncludeIndex int
	var lastIncludeTerm int
	//var lastApplied int
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil || d.Decode(&lastIncludeIndex) != nil || d.Decode(&lastIncludeTerm) != nil {
		// error...
		panic("fail to decode state")
	} else {
		rf.term = term
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludeIndex = lastIncludeIndex
		rf.lastIncludeTerm = lastIncludeTerm
		if rf.lastIncludeIndex > rf.lastApplied {
			rf.lastApplied = rf.lastIncludeIndex
		}
		if rf.lastIncludeIndex > rf.commitIndex {
			rf.commitIndex = rf.lastIncludeIndex
		}
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//

//snapshot是状态机对leader的行为
//installSnapshot 是leader 同步给 follower 让 follower同步给状态机的行为，此时follower本身的snapshot没有修改
//CondInstallSnapshot 是follower的状态机告诉follower修改snapshot以及log的行为
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIncludedIndex <= rf.lastIncludeIndex || lastIncludedTerm < rf.lastIncludeTerm {
		//fmt.Println("cond false", rf.me, lastIncludedIndex, rf.lastIncludeIndex, lastIncludedTerm, rf.lastIncludeTerm)

		return false
	}
	if lastIncludedIndex-rf.lastIncludeIndex >= len(rf.log) {
		rf.log = append([]LogEntry{}, LogEntry{
			Term: lastIncludedTerm,
		})
	} else {
		rf.log = append([]LogEntry{}, rf.log[lastIncludedIndex-rf.lastIncludeIndex:]...)
	}
	//fmt.Println("cond success", rf.me, rf.log)
	rf.lastIncludeIndex = lastIncludedIndex
	rf.lastIncludeTerm = lastIncludedTerm
	rf.snapshot = snapshot
	if rf.lastApplied < rf.lastIncludeIndex {
		rf.lastApplied = rf.lastIncludeIndex
	}
	if rf.commitIndex < rf.lastIncludeIndex {
		rf.commitIndex = rf.lastIncludeIndex
	}
	//rf.persist() //引入快照 log发生改变得持久化一下
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	//e.Encode(rf.lastApplied)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	//状态机给个index说 在这之前的所有log我都有了 你截断一下日志
	//raft就得截断一下日志 更新lastIncludeIndex(前提是index > rf.lastIncludeIndex)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Println("index    ", index, rf.lastIncludeIndex)
	if index <= rf.lastIncludeIndex {
		return
	}
	rf.log = append([]LogEntry{}, rf.log[index-rf.lastIncludeIndex:]...)
	rf.lastIncludeIndex = index
	rf.lastIncludeTerm = rf.log[0].Term
	rf.snapshot = snapshot
	//rf.lastApplied = rf.lastIncludeIndex
	if rf.lastIncludeIndex > rf.lastApplied {
		rf.lastApplied = rf.lastIncludeIndex
	}
	if rf.lastIncludeIndex > rf.commitIndex {
		rf.commitIndex = rf.lastIncludeIndex
	}
	//fmt.Println("leader snap", rf.lastIncludeIndex)
	//rf.persist() //引入快照 log发生改变得持久化一下
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	//e.Encode(rf.lastApplied)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)

}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Snapshot         []byte

	//server int
}

type InstallSnapshotReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendSnapshot(serverTo int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {

	ok := rf.peers[serverTo].Call("Raft.InstallSnapshot", args, reply)
	return ok
	//rf.persister.SaveStateAndSnapshot()
	//rf.nextIndex[id] = args.LastIncludeIndex + 1
	//rf.matchIndex[id] = args.LastIncludeTerm
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.term
	if args.Term < rf.term || args.LastIncludeIndex <= rf.lastIncludeIndex { //你不配当我leader或者我自己已经是最新快照了
		reply.Success = false
		rf.mu.Unlock()
		return
	}
	rf.electBeginTime = time.Now().Unix()
	if args.Term > rf.term {
		rf.ChangeState(follower, args.Term)
	}
	//if args.LastIncludeIndex <= rf.commitIndex {
	//	reply.Success = false
	//	return
	//}

	reply.Success = true
	rf.mu.Unlock()
	//fmt.Println(rf.me, "执行了快照apply", args.LastIncludeIndex)
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Snapshot,
		SnapshotTerm:  args.LastIncludeTerm,
		SnapshotIndex: args.LastIncludeIndex,
	}

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term       int
	ServerId   int
	FinalIndex int //最后一个日志项的index
	FinalTerm  int //其实就是term
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term  int
	Voted bool
}

//
// example RequestVote RPC handler.
//
//避免小term成为leader的方式是选举结束之后 马上发出心跳 遇到大term就会变成follower 此时又是无主模式 等待新一轮的选举
//所以选举放心大胆的按规则去选举就行
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//reply = &RequestVoteReply{}
	if args.Term < rf.term { //你的term比我的小
		reply.Voted = false
		reply.Term = rf.term
		return
	}
	//你的term比我的大 我得改成follower （此时我有可能是candidate/leader）同时我得更新的选举计时开始时间
	if args.Term > rf.term {
		rf.ChangeState(follower, args.Term) //这一步会重置votedFor为-1

		////fmt.Println("rf的term", rf.term)
		//reply.Term = rf.term

	}
	reply.Term = rf.term
	//判断这个节点有没有给其他的candidate投过票
	if (rf.votedFor == -1 || rf.votedFor == args.ServerId) && (len(rf.log) == 0 || (args.FinalTerm > rf.log[len(rf.log)-1].Term) ||
		(args.FinalTerm == rf.log[len(rf.log)-1].Term && args.FinalIndex-rf.lastIncludeIndex >= len(rf.log)-1)) { //注意当前日志项的判断
		reply.Voted = true

		rf.votedFor = args.ServerId
		rf.electBeginTime = time.Now().Unix() //只有投了票了才重置超时时间
		//fmt.Printf("[%d]: voted to [%d]\n", rf.me, args.ServerId)
		//fmt.Println("now reply", reply)
		rf.persist()
		return

	}
	//voted为false的情况: args.term == rf.term(没有重置votedFor) 并且rf给别人投过票了已经
	reply.Voted = false

}

func (rf *Raft) ChangeState(state string, args ...interface{}) {

	switch state {
	case follower:
		rf.term = args[0].(int)
		rf.votedFor = -1
		rf.persist()
		//fmt.Printf("[%d]server change state from %s to follower,now term is %d\n", rf.me, rf.state, rf.term)
	case candidate:
		rf.term++
		rf.votedFor = rf.me
		//fmt.Printf("[%d]server change state from %s to candidate\n", rf.me, rf.state)
		rf.electBeginTime = time.Now().Unix()
		rf.persist()
	case leader:
		//fmt.Printf("[%d]server change state from %s to leader\n", rf.me, rf.state)
		//转为leader的时候马上初始化matchIndex和nextIndex
		for i := 0; i < len(rf.matchIndex); i++ {
			rf.matchIndex[i] = -1
			rf.nextIndex[i] = len(rf.log) + rf.lastIncludeIndex //ggle
		}
		//rf.electBeginTime = time.Now().Unix()

	}
	rf.state = state

	return

}

func (rf *Raft) kickOffLeaderElection() {

	rf.ChangeState(candidate)
	totalNums := 1
	getNums := 1 //我自己
	numsLock := sync.Mutex{}
	//stateLock := sync.Mutex{}
	finalLogIndex := len(rf.log) - 1 //后面传参会加lastincludeindex
	finalLogTerm := -1
	if finalLogIndex >= 0 {
		finalLogTerm = rf.log[finalLogIndex].Term //注意不是rf.term 得是最后一个日志项对应的term 变成candidate的时候会给自己加一个term的
	}
	//var wg sync.WaitGroup
	//并发rpc发起投票请求 着重体现一个非顺序性 1这个节点可能先被candidate1请求 也有可能会先被candidate2请求
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			//rf.wg.Add(1)
			// 此处lastLogIndex及lastLogTerm在Lab 2A中无需关心。
			go func(serverTo int, term int, candidateId int, finalLogIndex int, finalLogTerm int) {
				//defer rf.wg.Done() // defer是FILO 最后执行
				//fmt.Println("开始")
				args := RequestVoteArgs{term, candidateId, finalLogIndex, finalLogTerm}
				reply := RequestVoteReply{}
				//fmt.Printf("[%d]: term: [%d], send request vote to: [%d]\n", candidateId, term, serverTo)
				ok := rf.sendRequestVote(serverTo, &args, &reply)
				//fmt.Println("reply  ", reply)
				numsLock.Lock()         //访问共有变量 开始上锁
				defer numsLock.Unlock() //defer不仅在return之后执行也可以在panic之后执行 所以释放锁最好在defer里面释放 以免程序panic 造成锁未能释放 导致死锁p
				totalNums++
				//fmt.Println("totalNums", totalNums)
				if !ok {
					//numsLock.Unlock()
					return
				}
				//	fmt.Println("reply term ", reply.Term, "    my term:", rf.term)
				//fmt.Println(reply.Voted)
				rf.mu.Lock() //访问共有变量 开始上锁
				defer rf.mu.Unlock()
				if reply.Voted && reply.Term == rf.term {
					getNums++
					//fmt.Println("get", getNums)
				}

				if reply.Term > rf.term {
					rf.ChangeState(follower, reply.Term)
					return
				}

				if getNums > len(rf.peers)/2 && rf.state == candidate {
					rf.ChangeState(leader)

					go rf.operateLeaderHeartbeat() //竞选成功后马上发送心跳
				}
				//	numsLock.Unlock()
				return

			}(i, rf.term, rf.me, finalLogIndex+rf.lastIncludeIndex, finalLogTerm)
		}
	}
	//rf.wg.Wait() //等待投票完成
	//fmt.Printf("getNums = %d\n", getNums)
	//if getNums > len(rf.peers)/2 && rf.state == candidate {
	//	rf.ChangeState(leader)
	//
	//	go rf.operateLeaderHeartbeat() //竞选成功后马上发送心跳
	//}

}
func (rf *Raft) operateLeaderHeartbeat() {
	for {

		if rf.killed() {
			return
		}
		rf.mu.Lock()
		if rf.state != leader {
			rf.mu.Unlock()
			return
		}

		for i := 0; i < len(rf.peers); i++ { //在某个协程的时候变成follower也硬着头皮把剩下的节点给日志append了 之后再判断rf.state != leader 才return掉
			if i != rf.me {
				prevLogIndex := rf.nextIndex[i] - 1 //下标next index 是要发送给follower的
				prevLogTerm := -1
				if prevLogIndex-rf.lastIncludeIndex >= 0 {
					prevLogTerm = rf.log[prevLogIndex-rf.lastIncludeIndex].Term
				}
				var entries []LogEntry
				//fmt.Println("le log len", len(rf.log))
				if len(rf.log)-1 >= rf.nextIndex[i]-rf.lastIncludeIndex && rf.nextIndex[i] >= rf.lastIncludeIndex { //一开始成为leader的时候 过不了这个限制 entries是空的
					//DPrintf("[%d]: len of log: %d, next index of [%d]: %d", rf.me, len(rf.log), i, rf.nextIndex[i])
					entries = append(entries, rf.log[rf.nextIndex[i]-rf.lastIncludeIndex:]...) //得是深拷贝 这个leader下台的时候 可能rf.log改变 导致entries改变
					//	fmt.Println("next index term", rf.log[rf.nextIndex[i]].Term)
				}
				//fmt.Println(len(entries))

				//唯一的leader给follower发快照的时机是leader已经没有对应的日志了
				//记住这之后也得更新nextIndex
				if rf.nextIndex[i] <= rf.lastIncludeIndex { //落后 直接补充到snapshot 此时follower状态机本身不知道快照 raft得等状态机确认快照后 调用condSnapshot才修改自身的snap
					//go rf.sendSnapshot(i, peer, InstallSnapshotArgs{
					//	Term:             rf.currentTerm,
					//	LastIncludeIndex: rf.log.Base,
					//	LastIncludeTerm:  rf.log.Entries[0].Term,
					//	Data:             rf.snapshot,
					//})
					//fmt.Println("n & l", rf.nextIndex[i], rf.lastIncludeIndex)
					//fmt.Println("快照n与l", rf.nextIndex[i], rf.lastIncludeIndex)
					go func(serverTo int, leaderTerm int, leaderLastIncludeTerm int, leaderLastIncludeIndex int, snapshot []byte) {
						args := InstallSnapshotArgs{Term: leaderTerm, LastIncludeTerm: leaderLastIncludeTerm, LastIncludeIndex: leaderLastIncludeIndex, Snapshot: snapshot}
						reply := InstallSnapshotReply{}
						ok := rf.sendSnapshot(serverTo, &args, &reply)
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if !ok {
							//fmt.Println("此时是rpc失败：", rf.me, serverTo)
							return
						}

						if reply.Term > rf.term {
							rf.ChangeState(follower, reply.Term)
							return
						}

						if reply.Success && rf.term == leaderTerm && reply.Term == rf.term { //修改nextIndex和matchIndex
							//	fmt.Println(rf.me, "给", serverTo, "发送快照", leaderLastIncludeIndex)
							rf.nextIndex[serverTo] = leaderLastIncludeIndex + 1
							//fmt.Println(rf.nextIndex, rf.lastIncludeIndex)
							rf.matchIndex[serverTo] = leaderLastIncludeIndex
						}
					}(i, rf.term, rf.lastIncludeTerm, rf.lastIncludeIndex, rf.snapshot)

				} else {
					// 此处prevLogIndex、prevLogTerm、entries和leaderCommit在Lab 2A中不需要关心
					//fmt.Println(prevLogIndex)
					go func(serverTo int, term int, leaderId int, prevLogIndex int, prevLogTerm int, entries []LogEntry, leaderCommit int) {
						args := AppendEntryArgs{term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit}
						reply := AppendEntryReply{}
						ok := rf.sendAppendEntry(serverTo, &args, &reply)

						rf.mu.Lock()
						defer rf.mu.Unlock()
						if !ok {
							//fmt.Println("此时是rpc失败：", rf.me, serverTo)
							return
						}

						if reply.Term > rf.term {

							rf.ChangeState(follower, reply.Term)

							return
						}

						//rf.term==term 代表我还是一个leader（我还没有被changeState成为follower）
						//fmt.Println("term 系列", rf.term, reply.Term, term)
						if rf.term == term && reply.Term == rf.term { //事实上最后一个日志项的term有可能小于rf.term(对应reply.success是false)，只是因为<的情况 在rpc调用的时候就更正为leader的term了
							if reply.Success { //pre与我的最后一个日志项一致
								rf.nextIndex[serverTo] = max(rf.nextIndex[serverTo], prevLogIndex+len(entries)+1)
								rf.matchIndex[serverTo] = max(rf.matchIndex[serverTo], prevLogIndex+len(entries))
								//fmt.Println(rf.me, "此时的reply success", serverTo, ":", rf.nextIndex[serverTo])
								currentMatches := make([]int, len(rf.peers))
								copy(currentMatches, rf.matchIndex)
								sort.Ints(currentMatches)           // 1(匹配1) 2（匹配1 2） 3（匹配 1 2 3） 4 5 6 7
								majority := (len(rf.peers) - 1) / 2 //向下取整 majority往下包括majority的下标 一定是大多数
								//fmt.Println(currentMatches, rf.commitIndex)
								for k := majority; k >= 0 && currentMatches[k] > rf.commitIndex; k-- {
									if rf.log[currentMatches[k]-rf.lastIncludeIndex].Term == rf.term { //leader只能提交自己任期内的日志项
										rf.commitIndex = currentMatches[k] //commitIndex面对的是状态机（记录的是一开始到现在的index值）
										rf.sendApplyMsg()                  //提交完了以后马上应用到上层的状态机
										//go rf.appMsgApplier()
										break
									}
								}

								//寻找中位数 然后提交
								//majority := len(rf.peers) / 2
								//sum := make(map[int]int)
								//for _, val := range currentMatches {
								//	if v, have := sum[val]; have {
								//		sum[val] = v + 1
								//	} else {
								//		sum[val] = 1
								//	}
								//}
								//for key, val := range sum {
								//	if val > majority && key >= 0 && rf.log[key].Term == rf.term {
								//		rf.commitIndex = key
								//		rf.sendApplyMsg()
								//	}
								//}

							} else {
								rf.nextIndex[serverTo] = prevLogIndex //原先的nextIndex-1
								//	fmt.Println("no success: a", rf.nextIndex[serverTo])
								if rf.nextIndex[serverTo]-1-rf.lastIncludeIndex >= reply.XLen && reply.XLen >= 0 {
									rf.nextIndex[serverTo] = reply.XLen + rf.lastIncludeIndex
									//fmt.Println("no success: b", serverTo, rf.nextIndex[serverTo])
								} else {
									for k := rf.nextIndex[serverTo] - 1; k >= reply.XIndex; k-- {
										if k > rf.lastIncludeIndex && rf.log[k-rf.lastIncludeIndex].Term != reply.XTerm && rf.nextIndex[serverTo] > 0 {
											rf.nextIndex[serverTo] -= 1
											//fmt.Println("no success: c", serverTo, rf.nextIndex[serverTo])
										} else { // xterm和现在nextIndex term一致的话 在rf.nextIndex[serverTo]-1 >= reply.XLen 的范围内 因为follower一定比leader短
											break
										}
									}
								}
							}
						}

					}(i, rf.term, rf.me, prevLogIndex, prevLogTerm, entries, rf.commitIndex)
				}
			}

		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(heartBeatInterval) * time.Millisecond)
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
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply) //server是serverTo arg里面的serverId是serverFrom 这样做方便广播发起投票 而不是每个server都在listen
	return ok
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
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

	if rf.killed() {
		return index, term, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = rf.state == leader
	if isLeader {
		rf.log = append(rf.log, LogEntry{Term: rf.term, Command: command})
		index = len(rf.log) - 1 + rf.lastIncludeIndex
		term = rf.term
		rf.matchIndex[rf.me] = len(rf.log) - 1 + rf.lastIncludeIndex
		rf.nextIndex[rf.me] = len(rf.log) + rf.lastIncludeIndex
		//DPrintf("[%d]: Start received command: index: %d, term: %d", rf.me, index, term)
		rf.persist()
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	sleepTime := time.Duration(electionInterval + rand.Int63n(timeoutSection))
	startTime := time.Now().Unix()
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(sleepTime * time.Millisecond)
		rf.mu.Lock()
		lastStartTime := startTime
		startTime = time.Now().Unix()
		predictTime := rf.electBeginTime + electionInterval + rand.Int63n(timeoutSection) //sleep期间更新了选举开始计时时间 每更新选举开始计时时间 就同时更新选举超时时间
		diff := predictTime - startTime                                                   //小于零代表当前这个时间已经过了 上次选举开始计时到结束的区间了 需要发起投票
		if lastStartTime > rf.electBeginTime || diff <= 0 {
			////DPrintf("[%d]: current state is [%s].", rf.me, rf.state)

			if rf.state != leader {
				// kickoff election
				//	fmt.Printf("[%d]: is not leader, start election.\n", rf.me)
				rf.kickOffLeaderElection() //发起选举
			}
			sleepTime = time.Duration(electionInterval + rand.Int63n(timeoutSection)) //选举完之后重置选举超时时间
		} else {
			sleepTime = time.Duration(diff)
		}
		rf.mu.Unlock()

	}
}
func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}
func (rf *Raft) GetSnapshot() []byte {
	return rf.persister.ReadSnapshot()
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
	rf.state = follower //初始都是follower 后面ticker会发起选举选出leader
	rf.term = 0
	rf.electBeginTime = -1
	rf.votedFor = -1

	// new code in Lab 2B
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{Term: -1}) //index从1开始 先把0占了

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastIncludeIndex = 0
	rf.lastIncludeTerm = -1

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyCh = applyCh
	rf.moreApply = false
	rf.applyCond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash

	rf.readPersist(persister.ReadRaftState())
	//rf.readPersist(persister.ReadSnapshot())

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.appMsgApplier()

	return rf
}
