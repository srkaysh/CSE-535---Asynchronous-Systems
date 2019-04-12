package raft

import (
	"log"
	"time"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const RPCMaxTries = 3
const RPCTimeout = 50 * time.Millisecond

func SendRPCRequest(request func() bool) bool {
	makeRequest := func(successChan chan struct{}) {
		if ok := request(); ok {
			successChan <- struct{}{}
		}
	}

	for attempts := 0; attempts < 1; attempts++ {
		rpcChan := make(chan struct{}, 1)
		go makeRequest(rpcChan)
		select {
		case <-rpcChan:
			return true
		case <-time.After(RPCTimeout):
		}
	}
	return false
}

// SendRPCRequest will attempt a request `RPCMaxTries` tries
// During testing, it is buggy, and not correct according to Raft Paper
func SendSnapshotRPCRequest(request func() bool) bool {
	makeRequest := func(successChan chan struct{}) {
		if ok := request(); ok {
			successChan <- struct{}{}
		}
	}

	for attempts := 0; attempts < 3; attempts++ {
		rpcChan := make(chan struct{}, 1)
		go makeRequest(rpcChan)
		select {
		case <-rpcChan:
			return true
		case <-time.After(RPCTimeout):
		}
	}
	return false
}
