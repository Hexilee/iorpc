//go:build !386
// +build !386

package iorpc

import (
	"sync/atomic"
)

// Snapshot returns connection statistics' snapshot.
//
// Use stats returned from ConnStats.Snapshot() on live Client and / or Server,
// since the original stats can be updated by concurrently running goroutines.
func (cs *ConnStats) Snapshot() *ConnStats {
	return &ConnStats{
		RPCCalls:     atomic.LoadUint64(&cs.RPCCalls),
		RPCTime:      atomic.LoadUint64(&cs.RPCTime),
		HeadWritten:  atomic.LoadUint64(&cs.HeadWritten),
		BodyWritten:  atomic.LoadUint64(&cs.BodyWritten),
		HeadRead:     atomic.LoadUint64(&cs.HeadRead),
		BodyRead:     atomic.LoadUint64(&cs.BodyRead),
		ReadCalls:    atomic.LoadUint64(&cs.ReadCalls),
		ReadErrors:   atomic.LoadUint64(&cs.ReadErrors),
		WriteCalls:   atomic.LoadUint64(&cs.WriteCalls),
		WriteErrors:  atomic.LoadUint64(&cs.WriteErrors),
		DialCalls:    atomic.LoadUint64(&cs.DialCalls),
		DialErrors:   atomic.LoadUint64(&cs.DialErrors),
		AcceptCalls:  atomic.LoadUint64(&cs.AcceptCalls),
		AcceptErrors: atomic.LoadUint64(&cs.AcceptErrors),
	}
}

// Reset resets all the stats counters.
func (cs *ConnStats) Reset() {
	atomic.StoreUint64(&cs.RPCCalls, 0)
	atomic.StoreUint64(&cs.RPCTime, 0)
	atomic.StoreUint64(&cs.HeadWritten, 0)
	atomic.StoreUint64(&cs.BodyWritten, 0)
	atomic.StoreUint64(&cs.HeadRead, 0)
	atomic.StoreUint64(&cs.BodyRead, 0)
	atomic.StoreUint64(&cs.WriteCalls, 0)
	atomic.StoreUint64(&cs.WriteErrors, 0)
	atomic.StoreUint64(&cs.ReadCalls, 0)
	atomic.StoreUint64(&cs.ReadErrors, 0)
	atomic.StoreUint64(&cs.DialCalls, 0)
	atomic.StoreUint64(&cs.DialErrors, 0)
	atomic.StoreUint64(&cs.AcceptCalls, 0)
	atomic.StoreUint64(&cs.AcceptErrors, 0)
}

func (cs *ConnStats) incRPCCalls() {
	atomic.AddUint64(&cs.RPCCalls, 1)
}

func (cs *ConnStats) incRPCTime(dt uint64) {
	atomic.AddUint64(&cs.RPCTime, dt)
}

func (cs *ConnStats) addHeadWritten(n uint64) {
	atomic.AddUint64(&cs.HeadWritten, n)
}

func (cs *ConnStats) addHeadRead(n uint64) {
	atomic.AddUint64(&cs.HeadRead, n)
}

func (cs *ConnStats) addBodyWritten(n uint64) {
	atomic.AddUint64(&cs.BodyWritten, n)
}

func (cs *ConnStats) addBodyRead(n uint64) {
	atomic.AddUint64(&cs.BodyRead, n)
}

func (cs *ConnStats) incReadCalls() {
	atomic.AddUint64(&cs.ReadCalls, 1)
}

func (cs *ConnStats) incReadErrors() {
	atomic.AddUint64(&cs.ReadErrors, 1)
}

func (cs *ConnStats) incWriteCalls() {
	atomic.AddUint64(&cs.WriteCalls, 1)
}

func (cs *ConnStats) incWriteErrors() {
	atomic.AddUint64(&cs.WriteErrors, 1)
}

func (cs *ConnStats) incDialCalls() {
	atomic.AddUint64(&cs.DialCalls, 1)
}

func (cs *ConnStats) incDialErrors() {
	atomic.AddUint64(&cs.DialErrors, 1)
}

func (cs *ConnStats) incAcceptCalls() {
	atomic.AddUint64(&cs.AcceptCalls, 1)
}

func (cs *ConnStats) incAcceptErrors() {
	atomic.AddUint64(&cs.AcceptErrors, 1)
}
