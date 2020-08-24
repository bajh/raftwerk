package raftlog

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sync"

	pb "github.com/bajh/raftwerk/transport"

	"github.com/golang/protobuf/proto"
)

type Log struct {
	// TODO: probably need to be able to seek in this?
	// should we just have the log entries after the commit buffered into memory?
	r         io.Reader
	w         io.Writer
	byteOrder binary.ByteOrder
	list      List
	LastIndex int64
	LastTerm  int64
	sync.RWMutex
}

type List []*pb.Entry

func (l *List) Len() int64 {
	return int64(len(*l))
}

func (l *List) GetEntry(index int64) *pb.Entry {
	if l.Len() <= index {
		return nil
	}
	return (*l)[index]
}

func (l *List) Slice(from int64, count int64) []*pb.Entry {
	to := from + count
	if to <= l.Len() {
		return (*l)[from:to]
	}
	return (*l)[from:]
}

func (l *List) DeleteFrom(index int64) {
	*l = (*l)[:index]
}

func (l *List) Insert(index int64, entry *pb.Entry) {
	// TODO: this function assumes the list's len is at least equal to index
	// if len(l) < index, what should happen? This is a programmer error, technically

	// If we're inserting an entry into the middle of the list, we first have to delete everything after the index
	// we're inserting
	if l.Len() > index {
		l.DeleteFrom(index)
	}
	*l = append(*l, entry)
	return
}

func NewFileLog(fileName string) *Log {
	r, err := os.OpenFile(fileName, os.O_RDONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Fatal("opening log for reading:", err)
	}
	w, err := os.OpenFile(fileName, os.O_WRONLY|os.O_APPEND, 0755)
	if err != nil {
		log.Fatal("opening log for writing:", err)
	}
	// TODO: read existing log from disk somewhere
	return &Log{
		r:         r,
		w:         w,
		byteOrder: binary.BigEndian,
		// TODO: these should come from the disk log
		LastIndex: -1,
		LastTerm:  -1,
	}
}

func NewMemLog(entries ...*pb.Entry) *Log {
	var b []byte
	w := bytes.NewBuffer(b)
	r := bytes.NewBuffer(b)
	return &Log{
		r:         r,
		w:         w,
		byteOrder: binary.BigEndian,
		list:      entries,
	}
}

func (l *Log) Len() int64 {
	return l.list.Len()
}

func (l *Log) Slice(from int64, count int64) []*pb.Entry {
	return l.list.Slice(from, count)
}

func (l *Log) GetEntry(index int64) *pb.Entry {
	// TODO: potentially need to get from file
	return l.list.GetEntry(index)
}

func (l *Log) LeaderAppend(entry *pb.Entry, term int64) error {
	entry.Term = term
	l.Lock()
	entry.Index = l.Len()
	l.Unlock()
	return l.Append(entry)
}

func (l *Log) Append(entry *pb.Entry) error {
	l.list.Insert(entry.GetIndex(), entry)
	l.LastIndex = entry.GetIndex()
	l.LastTerm = entry.GetTerm()
	return nil
}

func (l *Log) Vote(ctx context.Context, lastLogIndex string, lastLogTerm string) (bool, error) {
	return false, nil
}

func (l *Log) AppendIfSafe(ctx context.Context, entries []*pb.Entry, prevLogIndex int64, prevLogTerm int64) (bool, error) {
	if len(entries) == 0 {
		return true, nil
	}

	// Check for integrity of log up to this point
	if prevLogIndex >= 0 {
		// If the previous entry in the log conflicts with what the leader supplied, we cannot append
		prevEntry := l.GetEntry(prevLogIndex)
		if prevEntry == nil || prevEntry.GetTerm() != prevLogTerm {
			return false, nil
		}
	}

	// If the first entry being appended is already present in the log, we don't need to do anything else
	firstNewEntry := entries[0]
	currEntry := l.GetEntry(firstNewEntry.GetIndex())
	if currEntry != nil && currEntry.GetTerm() == firstNewEntry.GetTerm() {
		return true, nil
	}
	// otherwise, insert the new entries, overwriting entry at current index and any subsequent entries
	for _, entry := range entries {
		fmt.Fprintf(os.Stderr, "adding: %v\n", entry)
		l.Append(entry)
	}

	//b, err := proto.Marshal(entry)
	//if err != nil {
	//	return fmt.Errorf("marshaling entry: %v", err)
	//}
	//if err := binary.Write(l.w, l.byteOrder, int64(len(b))); err != nil {
	//	return fmt.Errorf("encoding len prefix: %v", err)
	//}
	//if err := binary.Write(l.w, l.byteOrder, b); err != nil {
	//	return fmt.Errorf("encoding message: %v", err)
	//}
	return true, nil
}

func (l *Log) ReadNext() (*pb.Entry, error) {
	var size int64
	if err := binary.Read(l.r, l.byteOrder, &size); err != nil {
		return nil, fmt.Errorf("reading len prefix: %w", err)
	}
	b := make([]byte, size)
	if err := binary.Read(l.r, l.byteOrder, b); err != nil {
		return nil, fmt.Errorf("reading: %v", err)
	}
	entry := &pb.Entry{}
	if err := proto.Unmarshal(b, entry); err != nil {
		return nil, fmt.Errorf("unmarshalling entry: %v", err)
	}
	return entry, nil
}
