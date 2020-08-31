// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	if storage == nil {
		panic("storage should be initialized")
	}
	fi, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	li, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	entries, err := storage.Entries(fi, li+1)
	if err != nil {
		panic(err)
	}
	return &RaftLog{
		storage:   storage,
		committed: fi - 1,
		applied:   fi - 1,
		stabled:   li,
		entries:   entries,
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		if l.stabled >= l.entries[len(l.entries)-1].Index {
			return nil
		}
		firstIndex := l.entries[0].Index
		return l.entries[l.stabled-firstIndex+1:]
	}
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) > 0 && l.applied < l.committed {
		firstIndex := l.entries[0].Index
		// firstIndex  applied  committed
		// 10          11 12 13 14
		// 0           1  2  3  4
		return l.entries[l.applied-firstIndex+1 : l.committed-firstIndex+1]
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	li, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return li
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) < 1 || l.entries[0].Index > i {
		return l.storage.Term(i)
	}
	if l.entries[len(l.entries)-1].Index < i {
		return 0, ErrUnavailable
	}
	return l.entries[i-l.entries[0].Index].Term, nil
}

func (l *RaftLog) AppliedTo(i uint64) {
	if i == 0 {
		return
	}
	if l.committed < i || i < l.applied {
		panic(fmt.Sprintf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed))
	}
	l.applied = i
}

func (l *RaftLog) matchTerm(index, term uint64) bool {
	t, err := l.Term(index)
	if err != nil {
		return false
	}
	return t == term
}

// findConflict finds the index of the conflict.
// It returns the first pair of conflicting entries between the existing
// entries and the given entries, if there are any.
// If there is no conflicting entries, and the existing entries contains
// all the given entries, zero will be returned.
// If there is no conflicting entries, but the given entries contains new
// entries, the index of the first new entry will be returned.
// An entry is considered to be conflicting if it has the same index but
// a different term.
// The first entry MUST have an index equal to the argument 'from'.
// The index of the given entries MUST be continuously increasing.
func (l *RaftLog) findConflict(ents []*pb.Entry) uint64 {
	for _, newEntry := range ents {
		if !l.matchTerm(newEntry.Index, newEntry.Term) {
			if newEntry.Index <= l.LastIndex() {
				log.Infof(fmt.Sprintf("found conflict at index %d [existing term: %d, conflicting term: %d]",
					newEntry.Index, l.zeroTermOnErrCompacted(l.Term(newEntry.Index)), newEntry.Term))
			}
			return newEntry.Index
		}
	}
	return 0
}

func (l *RaftLog) commitTo(tocommit uint64) {
	// never decrease commit
	if l.committed < tocommit {
		if l.LastIndex() < tocommit {
			panic(fmt.Sprintf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.LastIndex()))
		}
		l.committed = tocommit
	}
}

func (l *RaftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	panic(err)
	return 0
}

func (l *RaftLog) append(ents []*pb.Entry) {
	if len(ents) == 0 {
		return
	}
	after := ents[0].Index - 1
	if after < l.committed {
		panic(fmt.Sprintf("after(%d) is out of range [committed(%d)]", after, l.committed))

	}
	// if l.entries is empty, it would be captured in this branch
	if after == l.LastIndex()+1 {
		for _, entry := range ents {
			l.entries = append(l.entries, *entry)
		}
		return
	}
	if after > l.LastIndex()+1 {
		log.Panicf("after is too big")
	}
	if len(l.entries) < 1 {
		panic("entries shouldn't be empty here")
	}
	fi := l.entries[0].Index
	// 3,4,5
	// 1,2
	// 5,6,7
	// 2,3,4
	// TODO: ensure that whether ents[0].Index would less than fi
	if ents[0].Index < fi {
		if ents[0].Index+uint64(len(ents)) <= fi {
			panic("ents length small")
		}
		ents = ents[fi-ents[0].Index:]
	}
	// 2,3,4,5
	// 3,4,5
	l.entries = l.entries[:ents[0].Index-fi]
	for _, ent := range ents {
		l.entries = append(l.entries, *ent)
	}
}

// maybeAppend returns (0, false) if the entries cannot be appended. Otherwise,
// it returns (last index of new entries, true).
func (l *RaftLog) maybeAppend(prevIndex, prevlogTerm, committed uint64, ents []*pb.Entry) (lastnewi uint64, ok bool) {
	if l.matchTerm(prevIndex, prevlogTerm) {
		lastnewi = prevIndex + uint64(len(ents))
		ci := l.findConflict(ents)
		switch {
		case ci == 0:
		case ci <= l.committed:
			log.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
		default:
			// index 3
			// ents 4,5,6,7,8
			// ci 6,offset 3+1 = 4
			// ci-offset = 6-4 = 2
			// ents[ci-offset:] = 6,7,8
			offset := prevIndex + 1
			l.append(ents[ci-offset:])
		}
		l.commitTo(min(committed, lastnewi))
		return lastnewi, true
	}
	return 0, false
}
