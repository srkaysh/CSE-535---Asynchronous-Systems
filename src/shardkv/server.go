package shardkv

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"log"
	"raft"
	"shardmaster"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type DataBase map[string]string
type Duplicate map[int64]*LatestReply

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	Op       string // "Get", "Put" or "Append"
	ClientID int64  // client id
	SeqNo    int64  // request sequence number
}

// new config to switch
type Cfg struct {
	Config shardmaster.Config
}

// migrate data and dup table to spread
type Mig struct {
	Num   int
	Shard int
	Gid   int
	Data  DataBase
	Dup   Duplicate
}

type LatestReply struct {
	Seq   int      // latest request
	Reply GetReply // latest reply
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	mck           *shardmaster.Clerk    // talk to master
	persist       *raft.Persister       // store snapshot
	shutdownCh    chan struct{}         // shutdown gracefully
	db            DataBase              // data store
	notifyChs     map[int]chan struct{} // per log entry
	duplicate     Duplicate             // duplication detection table
	snapshotIndex int                   // snapshot
	configs       []shardmaster.Config  // 0 is always the current configuration
	workList      map[int]MigrateWork   // config No. -> work to be done
}

type MigrateWork struct {
	sendTo, recFrom []Item
}
type Item struct {
	shard, gid int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	// not leader ?
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	DPrintf("[%d-%d]: leader %d receive rpc: Get(%q).\n", kv.gid, kv.me, kv.me, args.Key)

	// not responsible for key?
	shard := key2shard(args.Key)
	kv.mu.Lock()
	if kv.configs[0].Shards[shard] != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}

	// if responsible, check whether already receive data from previous owner
	cur := kv.configs[0].Num
	if work, ok := kv.workList[cur]; ok {
		recFrom := work.recFrom
		for _, item := range recFrom {
			// still waiting data? postpone client request
			if shard == item.shard {
				kv.mu.Unlock()
				reply.Err = ErrWrongGroup
				return
			}
		}
	}

	// duplicate put/append request
	if dup, ok := kv.duplicate[args.ClientID]; ok {
		// filter duplicate
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = OK
			reply.Value = dup.Reply.Value
			return
		}
	}

	cmd := Op{Key: args.Key, Op: "Get", ClientID: args.ClientID, SeqNo: args.SeqNo}
	index, term, _ := kv.rf.Start(cmd)

	ch := make(chan struct{})
	kv.notifyChs[index] = ch
	kv.mu.Unlock()

	reply.WrongLeader = false
	reply.Err = OK

	// wait for Raft to complete agreement
	select {
	case <-ch:
		// lose leadership
		curTerm, isLeader := kv.rf.GetState()
		// what if still leader, but different term? let client retry
		if !isLeader || term != curTerm {
			reply.WrongLeader = true
			reply.Err = ""
			return
		}

		kv.mu.Lock()
		if value, ok := kv.db[args.Key]; ok {
			reply.Value = value
		} else {
			reply.Err = ErrNoKey
		}
		kv.mu.Unlock()
	case <-kv.shutdownCh:
		return
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// not leader ?
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	DPrintf("[%d-%d]: leader %d receive rpc: PutAppend(%q => (%q,%q), (%d-%d).\n", kv.gid, kv.me, kv.me,
		args.Op, args.Key, args.Value, args.ClientID, args.SeqNo)

	// not responsible for key?
	shard := key2shard(args.Key)
	kv.mu.Lock()
	if kv.configs[0].Shards[shard] != kv.gid {
		kv.mu.Unlock()
		reply.Err = ErrWrongGroup
		return
	}

	// if responsible, check whether already receive data from previous owner
	cur := kv.configs[0].Num
	if work, ok := kv.workList[cur]; ok {
		recFrom := work.recFrom
		for _, item := range recFrom {
			// still waiting data?
			if shard == item.shard {
				kv.mu.Unlock()
				// postpone client
				reply.Err = ErrWrongGroup
				return
			}
		}
	}

	// duplicate put/append request
	if dup, ok := kv.duplicate[args.ClientID]; ok {
		// filter duplicate
		if args.SeqNo <= dup.Seq {
			kv.mu.Unlock()
			reply.WrongLeader = false
			reply.Err = OK
			return
		}
	}

	// new request
	cmd := Op{Key: args.Key, Value: args.Value, Op: args.Op, ClientID: args.ClientID, SeqNo: args.SeqNo}
	index, term, _ := kv.rf.Start(cmd)
	ch := make(chan struct{})
	kv.notifyChs[index] = ch
	kv.mu.Unlock()

	reply.WrongLeader = false
	reply.Err = OK

	// wait for Raft to complete agreement
	select {
	case <-ch:
		// lose leadership
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

// Migrate Configuration
func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	// not leader?
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	// if receive unexpected newer config migrate, postpone?
	kv.mu.Lock()
	if args.Num != kv.configs[0].Num {
		kv.mu.Unlock()
		reply.WrongLeader = true
		reply.Err = ""
		return
	}

	DPrintf("[%d-%d]: leader %d receive rpc: Migrate, Num:%d, Shard: %d.\n", kv.gid, kv.me, kv.me,
		args.Num, args.Shard)

	// new request
	mig := Mig{Num: args.Num, Shard: args.Shard, Gid: args.Gid, Data: args.Data, Dup: args.Dup}
	index, term, _ := kv.rf.Start(mig)
	ch := make(chan struct{})

	kv.notifyChs[index] = ch
	kv.mu.Unlock()

	reply.WrongLeader = false
	reply.Shard = args.Shard
	reply.Err = OK

	// wait for Raft to complete agreement
	select {
	case <-ch:
		// lose leadership
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

// should be called when holding the lock
func (kv *ShardKV) applyMigratedData(mig Mig) {
	// update data
	for k, v := range mig.Data {
		if key2shard(k) == mig.Shard {
			kv.db[k] = v
		}
	}
	// update duplicate table
	for client, dup := range mig.Dup {
		d, ok := kv.duplicate[client]
		if ok {
			if d.Seq < dup.Seq {
				kv.duplicate[client].Seq = dup.Seq
			}
		} else {
			kv.duplicate[client] = dup
		}
	}
	// update workList
	if work, ok := kv.workList[mig.Num]; ok {
		recFrom := work.recFrom
		var done = -1
		for i, item := range recFrom {
			if item.shard == mig.Shard && item.gid == mig.Gid {
				done = i
				break
			}
		}
		if done != -1 {
			tmp := recFrom[done+1:]
			recFrom = recFrom[:done]
			recFrom = append(recFrom, tmp...)
			// update
			kv.workList[mig.Num] = MigrateWork{kv.workList[mig.Num].sendTo, recFrom}

			// if done, remove entry
			if len(kv.workList[mig.Num].sendTo) == 0 &&
				len(kv.workList[mig.Num].recFrom) == 0 {
				delete(kv.workList, mig.Num)
			}
		}
	}
	DPrintf("[%d-%d]: server %d, applyMigrateData: %v\n", kv.gid, kv.me, kv.me, kv.db)
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.shutdownCh)
}

// applyDaemon receive applyMsg from Raft layer, apply to Key-Value state machine
// then notify related client if it's leader
func (kv *ShardKV) applyDaemon() {
	for {
		select {
		case msg, ok := <-kv.applyCh:
			if ok {
				// have snapshot to apply?
				if msg.UseSnapshot {
					kv.mu.Lock()
					kv.readSnapshot(msg.Snapshot)
					// must be persisted, in case of crashing before generating another snapshot
					kv.generateSnapshot(msg.Index)
					kv.mu.Unlock()
					continue
				}
				// have client's request? must filter duplicate command
				if msg.Command != nil && msg.Index > kv.snapshotIndex {
					kv.mu.Lock()
					switch cmd := msg.Command.(type) {
					case Op:
						if dup, ok := kv.duplicate[cmd.ClientID]; !ok || dup.Seq < cmd.SeqNo {
							switch cmd.Op {
							case "Get":
								kv.duplicate[cmd.ClientID] = &LatestReply{Seq: cmd.SeqNo,
									Reply: GetReply{Value: kv.db[cmd.Key]}}
							case "Put":
								kv.db[cmd.Key] = cmd.Value
								kv.duplicate[cmd.ClientID] = &LatestReply{Seq: cmd.SeqNo}
							case "Append":
								kv.db[cmd.Key] += cmd.Value
								kv.duplicate[cmd.ClientID] = &LatestReply{Seq: cmd.SeqNo}
							default:
								DPrintf("[%d-%d]: server %d receive invalid cmd: %v\n", kv.gid, kv.me, kv.me, cmd)
								panic("invalid command operation")
							}
							if ok {
								DPrintf("[%d-%d]: server %d apply index: %d, cmd: %v (client: %d, dup seq: %d < %d)\n",
									kv.gid, kv.me, kv.me, msg.Index, cmd, cmd.ClientID, dup.Seq, cmd.SeqNo)
							}
							DPrintf("[%d-%d]: server %d, Op: %q, k-v: %q-%q, db: %v\n",
								kv.gid, kv.me, kv.me, cmd.Op, cmd.Key, cmd.Value, kv.db)
						}
					case Cfg:
						// duplicate detection: newer than current config
						// this point
						kv.switchConfig(cmd.Config)
						DPrintf("[%d-%d]: server %d switch to new config (%d->%d).\n",
							kv.gid, kv.me, kv.me, kv.configs[0].Num, cmd.Config.Num)
					case Mig:
						// apply data and dup, then start to accept client requests
						if cmd.Num == kv.configs[0].Num {
							kv.applyMigratedData(cmd)
						}
					default:
						panic("Oops... unknown cmd type from applyCh")
					}
					// snapshot detection: up through msg.Index
					if needSnapshot(kv) {
						// save snapshot and notify raft
						kv.generateSnapshot(msg.Index)
						kv.rf.NewSnapShot(msg.Index)
					}
					// notify channel
					if notifyCh, ok := kv.notifyChs[msg.Index]; ok && notifyCh != nil {
						close(notifyCh)
						delete(kv.notifyChs, msg.Index)
					}
					kv.mu.Unlock()
				}
			}
		case <-kv.shutdownCh:
			DPrintf("[%d-%d]: server %d is shutting down.\n", kv.gid, kv.me, kv.me)
			return
		}
	}
}

func needSnapshot(kv *ShardKV) bool {
	if kv.maxraftstate < 0 {
		return false
	}
	if kv.maxraftstate < kv.persist.RaftStateSize() {
		return true
	}
	// abs < 10% of max
	var abs = kv.maxraftstate - kv.persist.RaftStateSize()
	var threshold = kv.maxraftstate / 10
	if abs < threshold {
		return true
	}
	return false
}

// which index?
func (kv *ShardKV) generateSnapshot(index int) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	kv.snapshotIndex = index

	e.Encode(kv.db)
	e.Encode(kv.snapshotIndex)
	e.Encode(kv.duplicate)

	data := w.Bytes()
	kv.persist.SaveSnapshot(data)
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	kv.db = make(DataBase)
	kv.duplicate = make(Duplicate)

	d.Decode(&kv.db)
	d.Decode(&kv.snapshotIndex)
	d.Decode(&kv.duplicate)
}

// should be called when holding the lock
func (kv *ShardKV) switchConfig(new shardmaster.Config) {
	// for leader
	if _, isLeader := kv.rf.GetState(); isLeader && new.Num == kv.configs[0].Num {
		if work, ok := kv.workList[new.Num]; ok && len(work.sendTo) > 0 {
			// safe point to copy data and dup
			db, dup := kv.copyDataDup()
			go kv.sendShards(new, db, dup)
		}
		return
	}

	// for follower, switch to new config
	if new.Num > kv.configs[0].Num {
		if len(kv.configs) == 1 {
			kv.configs[0] = new
			DPrintf("[%d-%d]: config from leader.\n", kv.gid, kv.me)
		} else {
			if kv.configs[1].Num != new.Num {
				panic("possibly have missing configs")
			}
			kv.configs = kv.configs[1:]
			DPrintf("[%d-%d]: config from master.\n", kv.gid, kv.me)
		}
	}
}

// should be called when holding the lock
func (kv *ShardKV) copyDataDup() (DataBase, Duplicate) {
	data := make(DataBase)
	dup := make(Duplicate)
	for k, v := range kv.db {
		data[k] = v
	}
	for k, v := range kv.duplicate {
		reply := &LatestReply{v.Seq, v.Reply}
		dup[k] = reply
	}
	return data, dup
}

// using new config's Num as SeqNo.
func (kv *ShardKV) sendShards(config shardmaster.Config, db DataBase, dup Duplicate) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	sendTo := kv.workList[config.Num].sendTo

	fillMigrateArgs := func(shard int) *MigrateArgs {
		return &MigrateArgs{
			Num:   config.Num,
			Shard: shard,
			Gid:   kv.gid,
			Data:  db,
			Dup:   dup,
		}
	}
	replyHandler := func(reply *MigrateReply) {
		// still leader?
		if _, isLeader := kv.rf.GetState(); !isLeader {
			return
		}
		kv.mu.Lock()
		defer kv.mu.Unlock()

		// delete redundant data
		for k, _ := range kv.db {
			if key2shard(k) == reply.Shard {
				delete(kv.db, k)
			}
		}
		DPrintf("[%d-%d]: server %d delete shard: %d, db: %v\n", kv.gid, kv.me, kv.me, reply.Shard, kv.db)
	}
	// act as a normal client
	for _, task := range sendTo {
		go func(s, g int) {
			args := fillMigrateArgs(s)
			for {
				// still leader?
				if _, isLeader := kv.rf.GetState(); !isLeader {
					return
				}
				if servers, ok := config.Groups[g]; ok {
					// try each server for the shard.
					for si := 0; si < len(servers); si++ {
						srv := kv.make_end(servers[si])

						var reply MigrateReply
						ok := srv.Call("ShardKV.Migrate", args, &reply)
						if ok && reply.WrongLeader == false && reply.Err == OK {
							replyHandler(&reply)
							return
						}
					}
				}
				time.Sleep(100 * time.Millisecond)
			}
		}(task.shard, task.gid)
	}
	// update work list
	kv.workList[config.Num] = MigrateWork{nil, kv.workList[config.Num].recFrom}

	// if done, remove entry
	if len(kv.workList[config.Num].recFrom) == 0 {
		delete(kv.workList, config.Num)
	}
}

// should be called when holding the lock
func (kv *ShardKV) generateWorkList(old, new shardmaster.Config) {
	// 0 is initial state, no need to send any shards
	if old.Num == 0 {
		return
	}

	var os, ns = make(map[int]bool), make(map[int]bool)
	for s, g := range old.Shards {
		if g == kv.gid {
			os[s] = true
		}
	}
	for s, g := range new.Shards {
		if g == kv.gid {
			ns[s] = true
		}
	}
	var sendTo, recFrom []Item
	for k, _ := range os {
		if !ns[k] {
			sendTo = append(sendTo, Item{k, new.Shards[k]})
		}
	}
	for k, _ := range ns {
		if !os[k] {
			recFrom = append(recFrom, Item{k, old.Shards[k]})
		}
	}
	kv.workList[new.Num] = MigrateWork{sendTo, recFrom}
}

// get latest config: query shard master per 100ms
func (kv *ShardKV) getLatestConfig() {
	for {
		select {
		case <-kv.shutdownCh:
			return
		default:
			config := kv.mck.Query(-1)
			DPrintf("[%d-%d]: server %d detect new config: %d\n", kv.gid, kv.me, kv.me, config.Num)

			// config changed?
			kv.mu.Lock()
			last := len(kv.configs) - 1
			if isConfigChanged(kv.configs[last], config) {
				kv.configs = append(kv.configs, config)
			}
			// need waiting? just try, if failed, another leader will continue
			if len(kv.configs) > 1 {
				if _, isLeader := kv.rf.GetState(); isLeader && kv.isMigrateDone() {
					old, new := kv.configs[0], kv.configs[1]
					kv.generateWorkList(old, new)

					// leader will use new config ASAP
					kv.configs = kv.configs[1:]
					kv.rf.Start(Cfg{Config: new})
					DPrintf("[%d-%d]: leader %d detect new config (%d->%d).\n", kv.gid, kv.me, kv.me,
						old.Num, new.Num)
				}
			}
			kv.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func isConfigChanged(old, new shardmaster.Config) bool {
	return old.Num != new.Num || old.Shards != new.Shards
}

// should be called when holding the lock
func (kv *ShardKV) isMigrateDone() bool {
	_, ok := kv.workList[kv.configs[0].Num]
	return !ok
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots with
// persister.SaveSnapshot(), and Raft should save its state (including
// log) with persister.SaveRaftState().
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister,
	maxraftstate int, gid int, masters []*labrpc.ClientEnd,
	make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})
	gob.Register(Cfg{})
	gob.Register(Mig{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.persist = persister
	kv.shutdownCh = make(chan struct{})
	kv.db = make(DataBase)
	kv.notifyChs = make(map[int]chan struct{})
	kv.duplicate = make(Duplicate)

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.configs = []shardmaster.Config{{}}
	kv.workList = make(map[int]MigrateWork)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// long-running work
	go kv.applyDaemon()     // get log entry from raft layer
	go kv.getLatestConfig() // query shard master for configuration

	DPrintf("StartServer: %d-%d\n", kv.gid, kv.me)
	return kv
}
