package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type store interface {
	Get(key string) (value string, ok bool)
	Put(key string, value string) (newValue string)
	Append(key string, arg string) (newValue string)
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	CommandType CommandType //指令类型(put/append/get)
	Key         string      //键
	Value       string      //值(Get请求时此处为空)
	ClientId    int64       //client的唯一id
	CommandId   int         //命令的唯一id
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	clientReply    map[int64]CommandContext    //客户端的唯一指令和响应的对应map
	kvDataBase     KvDataBase                  //数据库,可自行定义和更换 其实就是内存中的一个map
	storeInterface store                       //数据库接口
	replyChMap     map[int]chan ApplyNotifyMsg //某index的响应的chan
	lastApplied    int                         //上一条应用的log的index,防止快照导致回退
}

// ApplyNotifyMsg 可表示GetReply和PutAppendReply
type ApplyNotifyMsg struct {
	Err   Err
	Value string //当Put/Append请求时此处为空
	//该被应用的command的term,便于RPC handler判断是否为过期请求(之前为leader并且start了,但是后来没有成功commit就变成了follower,导致一开始Start()得到的index处的命令不一定是之前的那个,所以需要拒绝掉;
	//或者是处于少部分分区的leader接收到命令,后来恢复分区之后,index处的log可能换成了新的leader的commit的log了
	Term int
}

type CommandContext struct {
	Command int            //该client的目前的commandId
	Reply   ApplyNotifyMsg //该command的响应
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	//defer kv.mu.Unlock()
	defer func() {
		DPrintf("kvserver[%d]: 返回Get RPC请求,args=[%v];Reply=[%v]\n", kv.me, args, reply)
	}()
	DPrintf("kvserver[%d]: 接收Get RPC请求,args=[%v]\n", kv.me, args)
	//1.先判断该命令是否已经被执行过了
	if commandContext, ok := kv.clientReply[args.ClientId]; ok {
		if commandContext.Command >= args.CommandId { //这个client最后执行的命令已经大于这个传参 代表这个命令也已经执行过了
			//若当前的请求已经被执行过了,那么直接返回结果
			reply.Err = commandContext.Reply.Err
			reply.Value = commandContext.Reply.Value
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()
	//2.若命令未被执行,那么开始生成Op并传递给raft
	op := Op{
		CommandType: GetMethod,
		Key:         args.Key,
		ClientId:    args.ClientId,
		CommandId:   args.CommandId,
	}
	index, term, isLeader := kv.rf.Start(op)
	//3.若不为leader则直接返回Err
	if !isLeader {
		reply.Err = ErrWrongLeader
		//kv.mu.Unlock()
		return
	}
	replyCh := make(chan ApplyNotifyMsg, 1)
	kv.mu.Lock()
	kv.replyChMap[index] = replyCh
	DPrintf("kvserver[%d]: 创建reply通道:index=[%d]\n", kv.me, index)
	kv.mu.Unlock()
	//4.等待应用后返回消息
	select {
	case replyMsg := <-replyCh:
		//当被通知时,返回结果
		DPrintf("kvserver[%d]: 获取到通知结果,index=[%d],replyMsg: %v\n", kv.me, index, replyMsg)
		if term == replyMsg.Term {
			reply.Err = replyMsg.Err
			reply.Value = replyMsg.Value
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		DPrintf("kvserver[%d]: 处理请求超时: %v\n", kv.me, op)
		reply.Err = ErrTimeout
	}
	//5.清除chan
	go kv.CloseChan(index)
}

func (kv *KVServer) CloseChan(index int) {
	kv.mu.Lock()
	//DPrintf("kvserver[%d]: 开始删除通道index: %d\n", kv.me, index)
	defer kv.mu.Unlock()
	ch, ok := kv.replyChMap[index]
	if !ok {
		//若该index没有保存通道,直接结束
		DPrintf("kvserver[%d]: 无该通道index: %d\n", kv.me, index)
		return
	}
	close(ch)
	delete(kv.replyChMap, index)
	DPrintf("kvserver[%d]: 成功删除通道index: %d\n", kv.me, index)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	//defer kv.mu.Unlock()
	defer func() {
		DPrintf("kvserver[%d]: 返回PutAppend RPC请求,args=[%v];Reply=[%v]\n", kv.me, args, reply)
	}()
	DPrintf("kvserver[%d]: 接收PutAppend RPC请求,args=[%v]\n", kv.me, args)
	//1.先判断该命令是否已经被执行过了
	if commandContext, ok := kv.clientReply[args.ClientId]; ok {
		//若该命令已被执行了,直接返回刚刚返回的结果
		if commandContext.Command == args.CommandId {
			//若当前的请求已经被执行过了,那么直接返回结果
			reply.Err = commandContext.Reply.Err
			DPrintf("kvserver[%d]: CommandId=[%d]==CommandContext.CommandId=[%d] ,直接返回: %v\n", kv.me, args.CommandId, commandContext.Command, reply)
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()
	//2.若命令未被执行,那么开始生成Op并传递给raft
	op := Op{
		CommandType: CommandType(args.Op),
		Key:         args.Key,
		Value:       args.Value,
		ClientId:    args.ClientId,
		CommandId:   args.CommandId,
	}
	index, term, isLeader := kv.rf.Start(op)
	//3.若不为leader则直接返回Err
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	replyCh := make(chan ApplyNotifyMsg, 1)
	kv.mu.Lock()
	kv.replyChMap[index] = replyCh //等待这个index在raft层达到一致
	DPrintf("kvserver[%d]: 创建reply通道:index=[%d]\n", kv.me, index)
	kv.mu.Unlock()
	//4.等待应用后返回消息
	select {
	case replyMsg := <-replyCh:
		//当被通知时,返回结果
		if term == replyMsg.Term {
			reply.Err = replyMsg.Err
		} else {
			reply.Err = ErrWrongLeader
		}
	case <-time.After(500 * time.Millisecond):
		//超时,返回结果,但是不更新Command -> Reply
		reply.Err = ErrTimeout
		DPrintf("kvserver[%d]: 处理请求超时: %v\n", kv.me, op)
	}
	go kv.CloseChan(index)
}

func (kv *KVServer) ReceiveApplyMsg() {
	for !kv.killed() {
		select {
		case applyMsg := <-kv.applyCh:
			DPrintf("kvserver[%d]: 获取到applyCh中新的applyMsg=[%v]\n", kv.me, applyMsg)
			//当为合法命令时
			if applyMsg.CommandValid {
				kv.ApplyCommand(applyMsg)
			} else if applyMsg.SnapshotValid {
				//当为合法快照时
				kv.ApplySnapshot(applyMsg)
			} else {
				//非法消息
				DPrintf("kvserver[%d]: error applyMsg from applyCh: %v\n", kv.me, applyMsg)
			}
		}
	}
}

func (kv *KVServer) ApplyCommand(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	var commonReply ApplyNotifyMsg
	op := applyMsg.Command.(Op)
	index := applyMsg.CommandIndex
	//当命令已经被应用过了
	if commandContext, ok := kv.clientReply[op.ClientId]; ok && commandContext.Command >= op.CommandId {
		DPrintf("kvserver[%d]: 该命令已被应用过,applyMsg: %v, commandContext: %v\n", kv.me, applyMsg, commandContext)
		commonReply = commandContext.Reply
		return
	}
	//当命令未被应用过
	if op.CommandType == GetMethod {
		//Get请求时
		if value, ok := kv.storeInterface.Get(op.Key); ok {
			//有该数据时
			commonReply = ApplyNotifyMsg{OK, value, applyMsg.CommandTerm}
		} else {
			//当没有数据时
			commonReply = ApplyNotifyMsg{ErrNoKey, value, applyMsg.CommandTerm}
		}
	} else if op.CommandType == PutMethod {
		//Put请求时
		value := kv.storeInterface.Put(op.Key, op.Value)
		commonReply = ApplyNotifyMsg{OK, value, applyMsg.CommandTerm}
	} else if op.CommandType == AppendMethod {
		//Append请求时
		newValue := kv.storeInterface.Append(op.Key, op.Value)
		commonReply = ApplyNotifyMsg{OK, newValue, applyMsg.CommandTerm}
	}
	//通知handler去响应请求
	if replyCh, ok := kv.replyChMap[index]; ok {
		DPrintf("kvserver[%d]: applyMsg: %v处理完成,通知index = [%d]的channel\n", kv.me, applyMsg, index)
		replyCh <- commonReply
		DPrintf("kvserver[%d]: applyMsg: %v处理完成,通知完成index = [%d]的channel\n", kv.me, applyMsg, index)
	}
	value, _ := kv.storeInterface.Get(op.Key)
	DPrintf("kvserver[%d]: 此时key=[%v],value=[%v]\n", kv.me, op.Key, value)
	//更新clientReply
	kv.clientReply[op.ClientId] = CommandContext{op.CommandId, commonReply}
	DPrintf("kvserver[%d]: 更新ClientId=[%d],CommandId=[%d],Reply=[%v]\n", kv.me, op.ClientId, op.CommandId, commonReply)
	kv.lastApplied = applyMsg.CommandIndex
	//判断是否需要快照
	if kv.needSnapshot() {
		kv.startSnapshot(applyMsg.CommandIndex)
	}
}

//判断当前是否需要进行snapshot(90%则需要快照)
func (kv *KVServer) needSnapshot() bool {
	if kv.maxraftstate == -1 {
		return false
	}
	var proportion float32
	proportion = float32(kv.rf.GetRaftStateSize() / kv.maxraftstate)
	return proportion > 0.9
}

//主动开始snapshot(由leader在maxRaftState不为-1,而且目前接近阈值的时候调用)
func (kv *KVServer) startSnapshot(index int) {
	DPrintf("kvserver[%d]: 容量接近阈值,进行快照,rateStateSize=[%d],maxRaftState=[%d]\n", kv.me, kv.rf.GetRaftStateSize(), kv.maxraftstate)
	snapshot := kv.createSnapshot()
	DPrintf("kvserver[%d]: 完成service层快照\n", kv.me)
	//通知Raft进行快照
	go kv.rf.Snapshot(index, snapshot)
}

//生成server的状态的snapshot
func (kv *KVServer) createSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	//编码kv数据
	err := e.Encode(kv.kvDataBase)
	if err != nil {
		log.Fatalf("kvserver[%d]: encode kvData error: %v\n", kv.me, err)
	}
	//编码clientReply(为了去重)
	err = e.Encode(kv.clientReply)
	if err != nil {
		log.Fatalf("kvserver[%d]: encode clientReply error: %v\n", kv.me, err)
	}
	snapshotData := w.Bytes()
	return snapshotData
}

// ApplySnapshot 被动应用snapshot
func (kv *KVServer) ApplySnapshot(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("kvserver[%d]: 接收到leader的快照\n", kv.me)
	if msg.SnapshotIndex < kv.lastApplied {
		DPrintf("kvserver[%d]: 接收到旧的日志,snapshotIndex=[%d],状态机的lastApplied=[%d]\n", kv.me, msg.SnapshotIndex, kv.lastApplied)
		return
	}
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		kv.lastApplied = msg.SnapshotIndex
		//将快照中的service层数据进行加载
		kv.readSnapshot(msg.Snapshot)
		DPrintf("kvserver[%d]: 完成service层快照\n", kv.me)
	}
}

// Kill
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
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer
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
	kv.clientReply = make(map[int64]CommandContext)
	kv.kvDataBase = KvDataBase{make(map[string]string)}
	kv.storeInterface = &kv.kvDataBase
	kv.replyChMap = make(map[int]chan ApplyNotifyMsg)
	//从快照中恢复数据
	kv.readSnapshot(kv.rf.GetSnapshot())
	go kv.ReceiveApplyMsg()
	return kv
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var kvDataBase KvDataBase
	var clientReply map[int64]CommandContext
	if d.Decode(&kvDataBase) != nil || d.Decode(&clientReply) != nil {
		DPrintf("kvserver[%d]: decode error\n", kv.me)
	} else {
		kv.kvDataBase = kvDataBase
		kv.clientReply = clientReply
		kv.storeInterface = &kvDataBase
	}
}
