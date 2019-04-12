package raftkv

import (
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	Key      string
	Value    string
	Op       string
	ClientId int64
	SeqId    int64
}

type LatestReply struct {
	seqid int64       // Last request
	reply interface{} // Last reply
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	db         map[string]string
	notifyChs  map[int]chan struct{}
	shutdownCh chan struct{}
	duplicate  map[int64]*LatestReply
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	DPrintf("[%d]: leader [%d] receive rpc: Get(%q).\n", kv.me, kv.me, args.Key)

	kv.mu.Lock()
	if dup, ok := kv.duplicate[args.ClientId]; ok {
		if args.SeqId <= dup.seqid {
			kv.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = OK
			reply.Value = dup.reply.(*GetReply).Value
			return
		}
	}

	cmd := Op{Key: args.Key, Op: "Get", ClientId: args.ClientId, SeqId: args.SeqId}
	index, term, _ := kv.rf.Start(cmd)
	ch := make(chan struct{})
	kv.notifyChs[index] = ch
	kv.mu.Unlock()

	reply.WrongLeader = false
	reply.Err = OK

	select {
	case <-ch:
		curTerm, isLeader := kv.rf.GetState()
		if !isLeader || term != curTerm {
			reply.WrongLeader = true
			reply.Err = ""
			return
		}

		kv.mu.Lock()
		delete(kv.notifyChs, index)
		if value, ok := kv.db[args.Key]; ok {
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
	case <-kv.shutdownCh:
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	DPrintf("[%d]: leader [%d] receive rpc: PutAppend([%q] => (%q,%q), (%d-%d).\n", kv.me, kv.me,
		args.Op, args.Key, args.Value, args.ClientId, args.SeqId)

	kv.mu.Lock()
	if dup, ok := kv.duplicate[args.ClientId]; ok {
		if args.SeqId <= dup.seqid {
			kv.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = OK
			return
		}
	}

	cmd := Op{Key: args.Key, Value: args.Value, Op: args.Op, ClientId: args.ClientId, SeqId: args.SeqId}
	index, term, _ := kv.rf.Start(cmd)
	ch := make(chan struct{})
	kv.notifyChs[index] = ch
	kv.mu.Unlock()

	reply.WrongLeader = false
	reply.Err = OK

	select {
	case <-ch:
		kv.mu.Lock()
		delete(kv.notifyChs, index)
		kv.mu.Unlock()

		curTerm, isLeader := kv.rf.GetState()
		if !isLeader || term != curTerm {
			reply.WrongLeader = true
			reply.Err = ""
			return
		}
	case <-kv.shutdownCh:
		return
	}

}

// applyDaemon receive applyMsg from Raft layer, apply to Key-Value state machine
// then notify related client if is leader
func (kv *KVServer) applyDaemon() {
	for {
		select {
		case <-kv.shutdownCh:
			DPrintf("[%d]: server [%d] is shutting down.\n", kv.me, kv.me)
			return
		case msg, ok := <-kv.applyCh:
			if ok {
				cmd := msg.Command.(Op)
				kv.mu.Lock()
				switch cmd.Op {
				case "Get":
					dup, ok := kv.duplicate[cmd.ClientId]
					if !ok || dup.seqid < cmd.SeqId {
						kv.duplicate[cmd.ClientId] = &LatestReply{seqid: cmd.SeqId,
							reply: &GetReply{Value: kv.db[cmd.Key]}}
					}
				case "Put":
					dup, ok := kv.duplicate[cmd.ClientId]
					if !ok || dup.seqid < cmd.SeqId {
						kv.db[cmd.Key] = cmd.Value
						kv.duplicate[cmd.ClientId] = &LatestReply{seqid: cmd.SeqId, reply: nil}
					}
				case "Append":
					dup, ok := kv.duplicate[cmd.ClientId]
					if !ok || dup.seqid < cmd.SeqId {
						kv.db[cmd.Key] += cmd.Value
						kv.duplicate[cmd.ClientId] = &LatestReply{seqid: cmd.SeqId, reply: nil}
					}
				default:
					DPrintf("[%d]: server [%d] receive invalid cmd: [%v]\n", kv.me, kv.me, cmd)
				}

				notifyCh := kv.notifyChs[msg.CommandIndex]
				kv.mu.Unlock()

				_, isLeader := kv.rf.GetState()
				if isLeader && notifyCh != nil {
					notifyCh <- struct{}{}
				} else if notifyCh != nil {
					close(notifyCh)
				}
			}
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	close(kv.shutdownCh)
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
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.db = make(map[string]string)
	kv.notifyChs = make(map[int]chan struct{})

	kv.shutdownCh = make(chan struct{})

	kv.duplicate = make(map[int64]*LatestReply)

	go kv.applyDaemon()
	return kv
}
