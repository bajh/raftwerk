package server

import (
	"container/heap"
	"context"
	"math"
	"sync"
)

// CommitTracker is responsible for determining when specific log indexes have been committed
// and informs any goroutines waiting for this index to be committed
type CommitTracker struct {
	// this can only be updated in one goroutine, which is the PropagateEntries goroutine
	commitIndex int64
	commitTicketHeap
	sync.RWMutex
}

// NewCommitTracker initializes a new CommitTracker
func NewCommitTracker() CommitTracker {
	return CommitTracker{
		commitIndex: -1,
	}
}

// CreateTicket creates a new CommitTicket whose Await function will return when commitIndex has
// successfully been committed. This will be called concurrently by the KeyValueStore operation handlers
func (c *CommitTracker) CreateTicket(commitIndex int64) *CommitTicket {
	ticket := &CommitTicket{
		commitIndex: commitIndex,
		c:           make(chan int64),
	}
	c.Lock()
	c.Push(ticket)
	c.Unlock()
	return ticket
}

// CommitTicket is issued through a call to CreateTicket to a gorutine that is interested in learning
// when a specific index has been committed
type CommitTicket struct {
	commitIndex int64
	c           chan int64
}

// Await blocks until the index that was passed to CreateTicket is committed
func (t *CommitTicket) Await(ctx context.Context) (int64, error) {
	// TODO: would it also be good if this had a way to bail out with an error that causes a redirect
	// if we stop being the leader?
	select {
	case <-ctx.Done():
		go func() {
			<-t.c
		}()
		return 0, context.Canceled
	case commitIndex := <-t.c:
		return commitIndex, nil
	}
}

type commitTicketHeap []*CommitTicket

func (h commitTicketHeap) Len() int           { return len(h) }
func (h commitTicketHeap) Less(i, j int) bool { return h[i].commitIndex < h[j].commitIndex }
func (h commitTicketHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *commitTicketHeap) Push(t interface{}) {
	*h = append(*h, t.(*CommitTicket))
}

func (h *commitTicketHeap) Pop() interface{} {
	old := *h
	n := len(old)
	t := old[n-1]
	*h = old[0 : n-1]
	return t
}

// signalTicketHolders finds any tickets that have been committed as a result of this commitIndex
// being reached (any tickets whose commitIndex is <= this commitIndex) and resolves them. It does
// this by popping off a heap until an Ticket is found with a commitIndex higher than commitIndex
func (c *CommitTracker) signalTicketHolders(commitIndex int64) {
	for c.Len() > 0 {
		c.Lock()
		t := heap.Pop(c).(*CommitTicket)
		c.Unlock()
		// if the commit represented by the ticket is done, send a signal
		if t.commitIndex <= commitIndex {
			t.c <- commitIndex
			// otherwise, put the ticket back and return
		} else {
			c.Lock()
			heap.Push(c, t)
			c.Unlock()
			return
		}
	}
}

// updateCommitIndex takes a matchIndex that was reached on a specific follower and determines
// whether that should become the new commitIndex (this will happen if this matchIndex has been
// reached on a majority of the nodes in the cluster)
func (c *CommitTracker) updateCommitIndex(newMatchIndex int64, matchIndexes []int64) int64 {
	if newMatchIndex <= c.commitIndex {
		return c.commitIndex
	}

	logsAtOrAboveMatchIndex := 0
	// TODO: this needs a lock
	// TODO: potentially use median approach, or short circuit iteration
	for _, followerMatchIndex := range matchIndexes {
		if followerMatchIndex >= newMatchIndex {
			logsAtOrAboveMatchIndex++
		}
	}
	need := math.Ceil(float64((len(matchIndexes) + 1)) / 2)
	if float64(logsAtOrAboveMatchIndex) >= need {
		c.commitIndex = newMatchIndex
		c.signalTicketHolders(c.commitIndex)
	}
	return c.commitIndex
}
