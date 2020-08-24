package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"

	"github.com/bajh/raftwerk/raftlog"
	"github.com/bajh/raftwerk/store"
	pb "github.com/bajh/raftwerk/transport"

	"google.golang.org/grpc"
)

type server struct {
	pb.UnimplementedRaftServer
	pb.UnimplementedKeyValStoreServer
	store   store.Store
	log     *raftlog.Log
	cluster *Cluster
	id      string
	*grpc.Server
}

func NewServer(id string, store store.Store, log *raftlog.Log, cluster *Cluster) *server {
	s := grpc.NewServer()
	raftServer := &server{
		id:      id,
		store:   store,
		log:     log,
		Server:  s,
		cluster: cluster,
	}

	pb.RegisterRaftServer(s, raftServer)
	pb.RegisterKeyValStoreServer(s, raftServer)

	return raftServer
}

func (s *server) ListenAndServe(port string) error {
	// TODO: I guess this should take a context? Is this even the right place to do this?
	// TODO: where should the starting term come from? Disk I suppose
	// TODO: there needs to be a mechanism to change votesNeeded when new clients are added...
	go s.cluster.stateMachine.Run(
		context.Background(),
		s.cluster,
		s.cluster, // pretty weird
		s.cluster.electionTimeoutTimer,
		int64(math.Ceil(float64(len(s.cluster.clients)+1)/2)),
	)
	ln, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}
	return s.Serve(ln)
}

func (s *server) replayLog() error {
	for {
		entry, err := s.log.ReadNext()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		ctx := context.Background()
		switch op := entry.Entry.(type) {
		case *pb.Entry_SetOp:
			if err := s.store.Set(
				ctx,
				op.SetOp.GetKey(),
				op.SetOp.GetVal(),
			); err != nil {
				return fmt.Errorf("performing set op: %v", err)
			}
		case *pb.Entry_DeleteOp:
			if err := s.store.Delete(
				ctx,
				op.DeleteOp.GetKey(),
			); err != nil {
				return fmt.Errorf("performing set op: %v", err)
			}
		}
	}
}

func (s *server) RequestVote(ctx context.Context, in *pb.VoteRequest) (*pb.VoteReply, error) {

	result := s.cluster.stateMachine.SendAndAwaitResult(RequestVoteRequest{
		Term:        in.GetTerm(),
		CandidateId: in.GetCandidateId(),
	})
	// IDEA: Wouldn't it be interesting if doing something like event.await() returned a kind of snapshot
	// of all the state you might need up to that point? Or maybe it would actually just return an EventResult.
	// It would apply the state update to the cluster state, which might
	// entail applying a state update to the log, then it would return you a reference to the state data you might care about.
	// It seems like it would make sense for these handlers to do NO state updating themselves, and all state updates would be processed
	// in serial by the StateMachine. This means the StateMachine is taking on more side effects, which makes it harder to test, but I think
	// we can make the receiver of these side effects a mockable interface.
	// However, the handlers would do NOTHING. They would just unwrap a GRPC call, pass an event into the state machine, receive a result
	// from the state machine, and send it back
	// Question. Should the StateMachine append entries to the log? If I'm going to follow the above design, it does have to be responsible
	// for writing its Term to the log, but should it also append to the log? I'm going to say yes. Haven't fully throught through if updating the
	// Raft role and writing to the log can be done in parallel at all

	return &pb.VoteReply{
		VoteGranted: result.Success,
		// I thought about this for a bit, whether this could be a term that's less than the Term the vote is requested for.
		// We HAVE to include the exact term being voted on to prevent client from receiving a stale term. Furthermore, we HAVE
		// to guarantee that we have committed a record of our vote for this term before continuing
		// And this means we need some way of finding out that our state transition actually occurred!!! We could add to State a channel
		// that sends after the state is processed. We could then have a function that wraps eventStream and creates a new channel,
		// adds it to the state, then returns it, allowing us to find out when our state is processed
		Term: result.Term,
	}, nil
}

func (s *server) AddPeer(ctx context.Context, in *pb.AddPeerRequest) (*pb.AddPeerReply, error) {
	peer, err := NewPeer(in.GetID(), in.GetHost())
	if err != nil {
		return nil, err
	}
	s.cluster.AddPeer(peer)
	return &pb.AddPeerReply{}, nil
}

func (s *server) AppendEntries(ctx context.Context, in *pb.AppendRequest) (*pb.AppendReply, error) {
	result := s.cluster.stateMachine.SendAndAwaitResult(AppendEntriesRequest{
		Term:         in.GetTerm(),
		PrevLogIndex: in.GetPrevLogIndex(),
		PrevLogTerm:  in.GetPrevLogTerm(),
		Entries:      in.GetEntries(),
	})

	return &pb.AppendReply{
		Success: result.Success,
		// I thought about this for a bit, whether this could be a term that's less than the Term the vote is requested for.
		// We HAVE to include the exact term being voted on to prevent client from receiving a stale term. Furthermore, we HAVE
		// to guarantee that we have committed a record of our vote for this term before continuing
		// And this means we need some way of finding out that our state transition actually occurred!!! We could add to State a channel
		// that sends after the state is processed. We could then have a function that wraps eventStream and creates a new channel,
		// adds it to the state, then returns it, allowing us to find out when our state is processed
		Term: result.Term,
	}, nil

	//success, err := s.log.AppendIfSafe(in.GetEntries(), in.GetPrevLogIndex(), in.GetPrevLogTerm())
	//if err != nil {
	//	return nil, fmt.Errorf("appending entries to log: %v", err)
	//}
	//return &pb.AppendReply{
	//	Term:    s.cluster.stateMachine.state.GetTerm(),
	//	Success: success,
	//}, nil
}

func (s *server) Get(ctx context.Context, in *pb.GetRequest) (*pb.GetReply, error) {
	val, err := s.store.Get(ctx, in.GetKey())
	if err != nil {
		return nil, fmt.Errorf("getting key: %v", err)
	}
	return &pb.GetReply{
		Val: val,
	}, nil
}

func setRequestToEntry(in *pb.SetRequest) *pb.Entry {
	return &pb.Entry{
		Entry: &pb.Entry_SetOp{
			SetOp: &pb.SetOp{
				Key: in.GetKey(),
				Val: in.GetVal(),
			},
		},
	}
}

func deleteRequestToEntry(in *pb.DeleteRequest) *pb.Entry {
	return &pb.Entry{
		Entry: &pb.Entry_DeleteOp{
			DeleteOp: &pb.DeleteOp{
				Key: in.GetKey(),
			},
		},
	}
}

func (s *server) Set(ctx context.Context, in *pb.SetRequest) (*pb.SetReply, error) {
	var term int64
	switch l := s.cluster.stateMachine.state.(type) {
	case *Leader:
		term = l.GetTerm()
	default:
		// TODO: redirect to the current leader
		return nil, fmt.Errorf("NOT the leader!")
	}

	entry := setRequestToEntry(in)

	// Check if we're the leader and get the term of our leadership?
	// Append to the log. Does the state machine need to know about this? Don't think so?
	if err := s.cluster.Log.LeaderAppend(entry, term); err != nil {
		return nil, fmt.Errorf("appending to log: %v", err)
	}
	// Get a ticket that will tell us when the entry is committed
	ticket := s.cluster.commitTracker.CreateTicket(entry.GetIndex())
	// Signal that there's a new entry to try to commit
	s.cluster.SignalEntry(entry)

	if _, err := ticket.Await(ctx); err != nil {
		return nil, err
	}

	// unclear when this is supposed to happen now? Maybe we need to block until this entry
	// has been committed to our own log (and receive its index), then we can stream commitIndex updates
	// and when we get the index we're interested in we can send back a success?
	// Hmm, maybe appending to the log actually happens synchronously, so we add to our own log and get back
	// a commit index and a channel that we'll receive over when our index is committed. That channel can be stored
	// on the cluster in a heap or something, and whenever a commit is made, we can pop off the heap and push to
	// the channel if our commit index is at least at that point. If it isn't, push back on the heap. Keep going until
	// we need to push something back on the heap.
	// type CommitSignal struct {commitIndex int64, chan<- struct{} }
	return &pb.SetReply{}, nil
}

func (s *server) Delete(ctx context.Context, in *pb.DeleteRequest) (*pb.DeleteReply, error) {
	entry := deleteRequestToEntry(in)
	if err := s.log.Append(entry); err != nil {
		return nil, fmt.Errorf("appending entry to log: %v", err)
	}
	if err := s.store.Delete(ctx, in.GetKey()); err != nil {
		return nil, fmt.Errorf("setting delete: %v", err)
	}
	return &pb.DeleteReply{}, nil
}
