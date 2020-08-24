package server

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/bajh/raftwerk/raftlog"
	pb "github.com/bajh/raftwerk/transport"
)

type Cluster struct {
	ID                  string
	clients             map[string]*Peer
	mu                  sync.Mutex
	transmissionTimeout time.Duration
	heartbeatTimer      TimerFunc
	appendTimeout       time.Duration
	*raftlog.Log
	maxEntrySize         int64
	electionTimeoutTimer TimerFunc
	stateMachine         *StateMachine
	commitTracker        CommitTracker
}

func NewCluster(log *raftlog.Log) *Cluster {
	return &Cluster{
		clients:             make(map[string]*Peer),
		transmissionTimeout: time.Second * 2,
		// TODO: parameterize timeout
		heartbeatTimer: func() <-chan time.Time {
			return time.After(time.Second * 5)
		},
		// TODO: parameterize - and how am I going to test this?
		appendTimeout: time.Second * 2,
		Log:           log,
		// TODO: make this configurable
		maxEntrySize: 3,
		// TODO: parameterize timeout
		electionTimeoutTimer: func() <-chan time.Time {
			return time.After(time.Second * time.Duration(10*(1+rand.Intn(1))))
		},
		stateMachine:  NewStateMachine(0),
		commitTracker: NewCommitTracker(),
	}
}

func (c *Cluster) SignalEntry(entry *pb.Entry) {
	for _, client := range c.clients {
		client.SignalEntry(entry)
	}
}

const BackoffBase = 32 * time.Millisecond
const BackoffMax = 4096 * time.Millisecond

func (c *Cluster) RequestVotes(ctx context.Context, cli *Peer, shutdown <-chan struct{}) {
	backoff := time.Millisecond * 0
	for {
		select {
		case <-ctx.Done():
			log.Println("Cancelled")
			<-shutdown
			return
		case <-shutdown:
			log.Println("Ending election loop for", cli.id)
			return
		case <-time.After(backoff):
		}

		// TODO: need some kind of logic to reconnect when connections are broken, but not sure where to put it
		if cli.grpc == nil {
			if err := cli.Dial(ctx); err != nil {
				log.Println("error dialing peer:", cli.id, err)
				continue
			}
		}

		resp, err := cli.grpc.RequestVote(ctx, &pb.VoteRequest{
			Term:         c.stateMachine.state.GetTerm(),
			CandidateId:  c.ID,
			LastLogIndex: c.Log.LastIndex,
			LastLogTerm:  c.Log.LastTerm,
		})
		if err != nil {
			log.Println("RequestVotes error:", err)
			switch {
			case backoff == 100:
				backoff = BackoffBase
			case backoff >= BackoffMax:
			default:
				backoff = backoff * 2
			}
			continue
		}
		// Should Term ALWAYS be equal to or greater than my term? Because if I send a term that's higher than
		// the server I'm talking to, it should increment its own term before responding to me
		c.stateMachine.Send(RequestVoteResponse{
			Term:        resp.GetTerm(),
			VoteGranted: resp.GetVoteGranted(),
		})
		<-shutdown
		return
	}
}

func (c *Cluster) StartPropagationLoop(ctx context.Context, cli *Peer, term int64, termStartEntryIndex int64, shutdown <-chan struct{}) {
	// We need to append a No-Op entry onto the log and receive the index of that entry. Maybe this can happen in the
	// Run function of the state machine and then it can be passed right into here? Yes, I think that's good - termStartEntryIndex will
	// represent that
	var nextIndex, matchIndex int64 = -1, -1
	for {
		nextIndex, matchIndex = c.PropagateEntries(ctx, cli, term, nextIndex, matchIndex)
		select {
		case <-ctx.Done():
			<-shutdown
			return
		case <-shutdown:
			return
		case <-cli.entryStream:
		case <-c.heartbeatTimer():
		}
	}
}

// The reason endTerm is being provided in addition to the ctx is the could also abort any
// in-flight GRPC requests, which I don't think is desired when a term ends? Not sure about this
// Need to confirm that it's safe for the rest of the function to continue even if a term has ended
// midway through
func (c *Cluster) PropagateEntries(ctx context.Context, cli *Peer, term int64, nextIndex int64, matchIndex int64) (newNextIndex int64, newMatchIndex int64) {

	// If the client's nextIndex is -1, it means we've never contacted the client before. Until
	// we've made a successful RPC call to the client, we'll need to keep submitting whatever
	// the current high watermark is for our own log until the client either indicates that we
	// should start replicating from this point or start to move backwards
	newNextIndex = func() int64 {
		if nextIndex == -1 {
			return c.Log.Len() - 1
		}
		return nextIndex
	}()

	// Get the next slice of entries and previous log entry info needed for this follower
	prevLogIndex, prevLogTerm, entries := func() (int64, int64, []*pb.Entry) {
		// nextIndex will be -1 when there's nothing to send
		// It will be 0 when we're starting with the first entry
		// In both cases, we'll say that the "previous entry" has the current term (so the term will
		// never cause a rejection) and that the previous index is -1 (because there's no prior entry)
		if nextIndex == -1 {
			return -1, term, []*pb.Entry{}
		}
		if nextIndex == 0 {
			return -1, term, c.Log.Slice(nextIndex, c.maxEntrySize)
		}
		// TODO: guard against this being nil (or it should return an err?)
		prevEntry := c.Log.GetEntry(nextIndex - 1)
		return prevEntry.GetIndex(), prevEntry.GetTerm(), c.Log.Slice(nextIndex, c.maxEntrySize)
	}()

	ctx, cancel := context.WithTimeout(ctx, c.appendTimeout)
	defer cancel()
	// TODO: perhaps handle this better
	if cli.grpc == nil {
		if err := cli.Dial(ctx); err != nil {
			log.Println("error dialing peer:", cli.id, err)
			return newNextIndex, matchIndex
		}
	}
	resp, err := cli.grpc.AppendEntries(ctx, &pb.AppendRequest{
		Term:         term,
		LeaderId:     c.ID,
		LeaderCommit: c.commitTracker.commitIndex,
		Entries:      entries,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
	})
	if err != nil {
		log.Printf("Error appending entries to %s: %v\n", cli.id, err)
		return newNextIndex, matchIndex
	}

	// TODO: somewhere...? we need to check if the follower told us we've been deposed
	// if we have, we need to return false

	// If AppendEntries succeeded, move nextIndex ptr ahead for the committed entries
	if resp.GetSuccess() {
		// TODO: perhaps this needs a lock later, but for now should be fine
		newNextIndex = newNextIndex + int64(len(entries))
		newMatchIndex = newNextIndex - 1
		cli.matchIndex = newMatchIndex
		// TODO: we can't commit an entry until we've committed the no-op from our term, so
		// we need to somehow keep track of the index of our no-op and never commit anything
		// earlier than that
		// c.updateCommitIndex(cli.matchIndex)
		c.stateMachine.Send(AppendEntriesResponse{
			Term:    resp.GetTerm(),
			Success: resp.GetSuccess(),
		})

		matchIndexes := make([]int64, len(c.clients))
		i := 0
		for _, follower := range c.clients {
			matchIndexes[i] = follower.matchIndex
			i++
		}
		c.commitTracker.updateCommitIndex(newMatchIndex, matchIndexes)

		return newNextIndex, newMatchIndex
	} else if resp.GetTerm() > term {
		// If this failed because it was from a prior term, inform the state machine - our days as leader are up
		c.stateMachine.Send(AppendEntriesResponse{
			Term: resp.GetTerm(),
		})
		return nextIndex, matchIndex
	}
	// Otherwise we're too far ahead in the log - move back
	return nextIndex - 1, matchIndex
}

func (c *Cluster) AddPeer(cli *Peer) {
	c.mu.Lock()
	c.clients[cli.id] = cli
	c.mu.Unlock()
}

func (c *Cluster) StartTermAsLeader(ctx context.Context, term int64) RunningState {
	// TODO: make any necessary changes to my own state?
	shutdowns := make([]chan<- struct{}, len(c.clients))

	i := 0
	for _, peer := range c.clients {
		ch := make(chan struct{})
		shutdowns[i] = ch
		go c.StartPropagationLoop(ctx, peer, term, -1, ch)

		//go func(ctx context.Context, peer *Peer, term int64) {
		//	var secure bool
		//	more := true
		//	for more {
		//		secure, more = c.PropagateEntries(ctx, peer, term, ch, secure)
		//	}
		//}(ctx, peer, term)
		i++
	}
	return NewRunningState(shutdowns)
}

func (c *Cluster) StartElection(ctx context.Context) RunningState {
	shutdowns := make([]chan<- struct{}, len(c.clients))

	i := 0
	for _, peer := range c.clients {
		ch := make(chan struct{})
		shutdowns[i] = ch
		go c.RequestVotes(ctx, peer, ch)
		i++
	}
	return NewRunningState(shutdowns)
}

type TimerFunc func() <-chan time.Time

type Peer struct {
	id          string
	addr        string
	grpc        pb.RaftClient
	conn        *grpc.ClientConn
	alive       bool
	mu          sync.RWMutex
	nextIndex   int64 // This starts at my log's current index
	matchIndex  int64 // This starts at -1 and eventually reaches the follower's log's index
	entryStream chan *pb.Entry
}

func (c *Peer) SignalEntry(entry *pb.Entry) {
	go func() {
		select {
		case c.entryStream <- entry:
		default:
		}
	}()
}

func NewPeer(id string, addr string) (*Peer, error) {
	return &Peer{
		id:          id,
		addr:        addr,
		alive:       true, // TODO: I don't think this is an actual thing
		entryStream: make(chan *pb.Entry, 1),
		nextIndex:   -1,
		matchIndex:  -1,
	}, nil
}

func (p *Peer) Dial(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	conn, err := grpc.DialContext(ctx, p.addr, grpc.WithInsecure(), grpc.WithBlock())
	// TODO: if we can't connect, we should just set alive to false and keep retrying until we can
	// TODO: And what about if the connection gets broken? Reconnect somewhere?
	if err != nil {
		return fmt.Errorf("could not connect: %v", err)
	}
	cli := pb.NewRaftClient(conn)
	p.grpc = cli
	p.conn = conn
	return nil
}
