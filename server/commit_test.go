package server

import (
	"context"
	"testing"
)

func TestUpdateCommitIndex(t *testing.T) {
	type Test struct {
		NewMatchIndex int64
		MatchIndexes  []int64
		CommitIndex   int64
	}
	tests := []Test{
		{3, []int64{1, 1, 2}, -1},
		{3, []int64{1, 3, 4}, 3},
		{5, []int64{5, 5, 3, 3}, 3},
		{5, []int64{5, 5, 9, 3}, 5},
	}
	c := NewCommitTracker()

	for i, test := range tests {

		commitIndex := c.updateCommitIndex(test.NewMatchIndex, test.MatchIndexes)
		if commitIndex != test.CommitIndex {
			t.Errorf(
				"[%d] expected to reach commit index %d, got %d",
				i,
				test.CommitIndex,
				commitIndex,
			)
		}
	}
}

func TestCommitTracker(t *testing.T) {
	c := NewCommitTracker()

	ticket3 := c.CreateTicket(3)
	ticket4 := c.CreateTicket(4)
	ticket5 := c.CreateTicket(5)
	ctx := context.Background()
	done := make(chan struct{})
	go func() {
		c.updateCommitIndex(4, []int64{4, 4, 3})
		done <- struct{}{}
	}()
	if _, err := ticket3.Await(ctx); err != nil {
		t.Errorf("%v", err)
	}
	if _, err := ticket4.Await(ctx); err != nil {
		t.Errorf("%v", err)
	}
	<-done
	go func() {
		c.updateCommitIndex(5, []int64{5, 5, 5})
		done <- struct{}{}
	}()
	if _, err := ticket5.Await(ctx); err != nil {
		t.Errorf("%v", err)
	}
	<-done
}
