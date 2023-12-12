package raft

const (
	follower                = "follower"
	candidate               = "candidate"
	leader                  = "leader"
	electionInterval  int64 = 300
	timeoutSection    int64 = 300
	heartBeatInterval int64 = 33
)
