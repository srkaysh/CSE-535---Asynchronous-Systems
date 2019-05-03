package shardmaster

import (
	"labgob"
	"labrpc"
	"raft"
	"sync"
	"time"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	configs []Config // indexed by config num

	requestHandlers map[int]chan raft.ApplyMsg
	latestRequests  map[int64]int64 // Client ID -> Last applied Request ID
}

type Op struct {
	// Your data here.
	Method  string
	Configs []Config
}

func (sm *ShardMaster) isRequestDuplicate(clientId int64, requestId int64) bool {
	lastRequest, isPresent := sm.latestRequests[clientId]
	sm.latestRequests[clientId] = requestId
	return isPresent && lastRequest == requestId
}

func (sm *ShardMaster) await(index int, op Op) (success bool) {

	sm.mu.Lock()
	awaitChan := make(chan raft.ApplyMsg, 1)
	sm.requestHandlers[index] = awaitChan
	sm.mu.Unlock()

	for {
		select {
		case message := <-awaitChan:
			if index == message.CommandIndex /*&& op == message.Command*/ {
				close(awaitChan)
				return true
			} else {
				return false
			}
		case <-time.After(800 * time.Millisecond):
			return false
		}
	}
}

func (sm *ShardMaster) rebalance(groups map[int][]string) {
	config := Config{}
	config.Num = len(sm.configs)
	config.Groups = groups

	// balance shards to latest groups
	numOfGroup := len(groups)
	if numOfGroup > 0 {
		leftOver := NShards % numOfGroup

		i := 0
		for i < NShards-leftOver {
			for gid, _ := range groups {
				config.Shards[i] = gid
				i++
			}
		}

		groupList := make([]int, 0)
		for gid, _ := range groups {
			groupList = append(groupList, gid)
		}

		// add left over shards
		for j := NShards - leftOver; j < NShards && len(groupList) > 0; j++ {
			nextGroup := (j % numOfGroup)
			config.Shards[j] = groupList[nextGroup]
		}

	}
	sm.configs = append(sm.configs, config)
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sm.mu.Lock()

	if sm.isRequestDuplicate(args.ClientId, args.RequestId) {
		sm.mu.Unlock()
		return
	}

	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		sm.mu.Unlock()
		return
	}

	groups := make(map[int][]string)

	// Go maps are references. If you assign one variable of type map to another,
	// both variables refer to the same map. Thus if you want to create a new Config based on
	// a previous one, you need to create a new map object (with make())
	// and copy the keys and values individually.
	preCfg := sm.configs[len(sm.configs)-1]
	for i, names := range preCfg.Groups {
		groups[i] = names
	}

	// add new groups
	for serIndex, names := range args.Servers {
		groups[serIndex] = names
	}

	sm.rebalance(groups)

	ops := Op{
		Method:  "Join",
		Configs: sm.configs,
	}

	sm.mu.Unlock()
	index, _, isLeader := sm.rf.Start(ops)

	if !isLeader {
		reply.WrongLeader = true
	} else {
		success := sm.await(index, ops)
		if !success {
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
			reply.Err = OK
		}
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sm.mu.Lock()
	if sm.isRequestDuplicate(args.ClientId, args.RequestId) {
		sm.mu.Unlock()
		return
	}

	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		sm.mu.Unlock()
		return
	}

	groups := make(map[int][]string)

	// Go maps are references. If you assign one variable of type map to another,
	// both variables refer to the same map. Thus if you want to create a new Config based on
	// a previous one, you need to create a new map object (with make())
	// and copy the keys and values individually.
	preCfg := sm.configs[len(sm.configs)-1]
	isGidInLeaveGroups := func(gid int) bool {
		for _, g := range args.GIDs {
			if gid == g {
				return true
			}
		}
		return false
	}
	for i, names := range preCfg.Groups {
		if !isGidInLeaveGroups(i) {
			groups[i] = names
		}
	}

	sm.rebalance(groups)

	ops := Op{
		Method:  "Leave",
		Configs: sm.configs,
	}
	sm.mu.Unlock()

	index, _, isLeader := sm.rf.Start(ops)

	if !isLeader {
		reply.WrongLeader = true
	} else {
		success := sm.await(index, ops)
		if !success {
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
			reply.Err = OK
		}
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sm.mu.Lock()
	if sm.isRequestDuplicate(args.ClientId, args.RequestId) {
		sm.mu.Unlock()
		return
	}

	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		sm.mu.Unlock()
		return
	}

	config := Config{}
	config.Num = len(sm.configs)
	config.Groups = make(map[int][]string)

	preCfg := sm.configs[len(sm.configs)-1]
	for i, names := range preCfg.Groups {
		config.Groups[i] = names
	}
	config.Shards = preCfg.Shards
	config.Shards[args.Shard] = args.GID

	ops := Op{
		Method:  "Move",
		Configs: sm.configs,
	}
	sm.configs = append(sm.configs, config)
	sm.mu.Unlock()

	index, _, isLeader := sm.rf.Start(ops)

	if !isLeader {
		reply.WrongLeader = true
	} else {
		success := sm.await(index, ops)
		if !success {
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
			reply.Err = OK
		}
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sm.mu.Lock()
	if !sm.rf.IsLeader() {
		reply.WrongLeader = true
		sm.mu.Unlock()
		return
	}

	ops := Op{
		Method:  "Query",
		Configs: sm.configs,
	}
	sm.mu.Unlock()

	index, _, isLeader := sm.rf.Start(ops)

	if !isLeader {
		reply.WrongLeader = true
	} else {
		success := sm.await(index, ops)
		if !success {
			reply.WrongLeader = true
		} else {
			reply.WrongLeader = false
			reply.Err = OK

			if args.Num == -1 || args.Num > len(sm.configs) {
				reply.Config = sm.configs[len(sm.configs)-1]
			} else {
				for i, cfg := range sm.configs {
					if args.Num == i {
						reply.Config = cfg
						break
					}
				}
			}
		}
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) periodCheckApplyMsg() {
	for {
		select {
		case m, ok := <-sm.applyCh:
			if ok {
				if m.Command != nil {
					sm.mu.Lock()
					cmd := m.Command.(Op)
					if _, isLeader := sm.rf.GetState(); !isLeader {
						sm.configs = cmd.Configs
					}

					// When we have applied message, we found the waiting channel(issued by RPC handler), forward the Ops
					if c, ok := sm.requestHandlers[m.CommandIndex]; ok {
						c <- m
						delete(sm.requestHandlers, m.CommandIndex)
					}
					sm.mu.Unlock()
				}
			}
		}
	}
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.requestHandlers = make(map[int]chan raft.ApplyMsg)
	sm.latestRequests = make(map[int64]int64)

	go sm.periodCheckApplyMsg()

	return sm
}
