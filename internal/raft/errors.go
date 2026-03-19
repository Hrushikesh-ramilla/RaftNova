package raft

import "errors"

var (
	ErrNotLeader = errors.New("not the leader")
	ErrTimeout   = errors.New("request timed out")
	ErrStopped   = errors.New("node is stopped")
)
