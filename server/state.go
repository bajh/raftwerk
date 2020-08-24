package server

import (
	"context"
	"log"
	"time"

	pb "github.com/bajh/raftwerk/transport"
)

type Log interface {
	AppendIfSafe(ctx context.Context, entries []*pb.Entry, prevLogIndex int64, prevLogTerm int64) (bool, error)
	Vote(ctx context.Context, lastLogIndex string, lastLogTerm string) (bool, error)
}

type StateTaskManager interface {
	StartElection(ctx context.Context) RunningState
	StartTermAsLeader(ctx context.Context, term int64) RunningState
}

type State interface {
	// Apply takes an event and applies it to the State to produce a new State, if
	// a transition needs to occur. The state returns itself if no transition occurs
	Apply(ctx context.Context, event StateEvent, t *timer, votesNeed int64, lg Log) (State, *EventResult)
	GetTerm() int64
	Run(ctx context.Context, stateTaskManager StateTaskManager)
}

type RunningState struct {
	shutdowns []chan<- struct{}
}

func NewRunningState(shutdowns []chan<- struct{}) RunningState {
	return RunningState{
		shutdowns: shutdowns,
	}
}

func (s RunningState) Stop() {
	for _, shutdown := range s.shutdowns {
		shutdown <- struct{}{}
	}
}

type timer struct {
	C          <-chan ElectionTimeout
	startTimer func() <-chan ElectionTimeout
}

func (t *timer) Pause() {
	t.C = nil
}

func (t *timer) Start() {
	t.C = t.startTimer()
}

func NewTimer(f TimerFunc) *timer {
	// channel is buffered so if no one ends up waiting on the channel (because the
	// timer is replaced by a new one) it'll be GC'ed
	startTimer := func() <-chan ElectionTimeout {
		timedEventChan := make(chan ElectionTimeout, 1)
		go func() {
			time := <-f()
			timedEventChan <- ElectionTimeout{
				Time: time,
			}
		}()
		return timedEventChan
	}

	return &timer{
		C:          startTimer(),
		startTimer: startTimer,
	}
}

type StateMachine struct {
	c     chan StateEvent
	state State
}

func NewStateMachine(term int64) *StateMachine {
	c := make(chan StateEvent)
	state := &Follower{
		Term: term,
	}
	return &StateMachine{
		c:     c,
		state: state,
	}
}

func (m *StateMachine) Run(
	ctx context.Context,
	stateTaskManager StateTaskManager,
	log Log,
	electionTimeoutTimer TimerFunc,
	votesNeeded int64,
) {
	electionTimer := NewTimer(electionTimeoutTimer)
	for {
		var event StateEvent
		select {
		case <-ctx.Done():
			return
		case event = <-m.c:
		case event = <-electionTimer.startTimer():
		}
		nextState, result := m.state.Apply(ctx, event, electionTimer, votesNeeded, log)
		if m.state != nextState {
			nextState.Run(ctx, stateTaskManager)
		}

		go event.publishResult(result)
		m.state = nextState
	}
}

func (m *StateMachine) SendAndAwaitResult(event StateEvent) *EventResult {
	resultC := make(chan *EventResult)
	go func() {
		m.c <- event.withResultC(resultC)
	}()
	return <-resultC
}

func (m *StateMachine) Send(event StateEvent) {
	go func() {
		m.c <- event
	}()
}

type StateEvent interface {
	GetTerm() int64
	withResultC(chan *EventResult) StateEvent
	publishResult(e *EventResult)
}

// EventResult is kind of a bundle of data that isn't always relevant, so maybe a better way to do this. If we had generics
// we could relate this to the type of event being enqueued, but since we don't the alternative would maybe have to be an interface{}
// that gets converted into the appropriate result type... Keep thinking about this
type EventResult struct {
	Term    int64
	Success bool
	Err     error
}

func (e AppendEntriesRequest) withResultC(c chan *EventResult) StateEvent {
	e.c = c
	return e
}

func (e AppendEntriesRequest) publishResult(r *EventResult) {
	// I think this makes sense... no need for the state machine to wait for the result of this transition to be consumed
	if e.c != nil {
		go func() {
			e.c <- r
		}()
	}
}

func (e AppendEntriesResponse) withResultC(c chan *EventResult) StateEvent {
	e.c = c
	return e
}

func (e AppendEntriesResponse) publishResult(r *EventResult) {
	// I think this makes sense... no need for the state machine to wait for the result of this transition to be consumed
	if e.c != nil {
		go func() {
			e.c <- r
		}()
	}
}

type AppendEntriesRequest struct {
	Term         int64
	Entries      []*pb.Entry
	PrevLogTerm  int64
	PrevLogIndex int64
	c            chan *EventResult
}

func (r AppendEntriesRequest) GetTerm() int64 {
	return r.Term
}

type AppendEntriesResponse struct {
	Term    int64
	Success bool
	c       chan *EventResult
}

func (r AppendEntriesResponse) GetTerm() int64 {
	return r.Term
}

type ElectionTimeout struct {
	Time time.Time
	c    chan *EventResult
}

func (e ElectionTimeout) withResultC(c chan *EventResult) StateEvent {
	e.c = c
	return e
}

func (e ElectionTimeout) publishResult(r *EventResult) {
	// I think this makes sense... no need for the state machine to wait for the result of this transition to be consumed
	if e.c != nil {
		go func() {
			e.c <- r
		}()
	}
}

func (e ElectionTimeout) GetTerm() int64 {
	// So far I haven't come up with a use for this but it's convenient to have all
	// events have GetTerm()... I believe that any time an ElectionTimeout happens
	// it is guaranteed to be for the current term, but if there are issues with this
	// assumption could start keeping track of the timeout Term for debugging...
	return 0
}

type RequestVoteRequest struct {
	Term        int64
	CandidateId string
	c           chan *EventResult
}

func (e RequestVoteRequest) withResultC(c chan *EventResult) StateEvent {
	e.c = c
	return e
}

func (e RequestVoteRequest) publishResult(r *EventResult) {
	// I think this makes sense... no need for the state machine to wait for the result of this transition to be consumed
	if e.c != nil {
		go func() {
			e.c <- r
		}()
	}
}

func (e RequestVoteRequest) GetTerm() int64 {
	return e.Term
}

type RequestVoteResponse struct {
	Term        int64
	VoteGranted bool
	c           chan *EventResult
}

func (e RequestVoteResponse) withResultC(c chan *EventResult) StateEvent {
	e.c = c
	return e
}

func (e RequestVoteResponse) publishResult(r *EventResult) {
	// I think this makes sense... no need for the state machine to wait for the result of this transition to be consumed
	if e.c != nil {
		go func() {
			e.c <- r
		}()
	}
}

func (r RequestVoteResponse) GetTerm() int64 {
	return r.Term
}

type Leader struct {
	Term      int64
	shutdowns []chan<- struct{}
	RunningState
}

// if you're a follower, you must reset your election timer when:
// * you convert to candidate (your timer expires)
// * you start a new term (find out about a higher term from AppendEntries)
// * you receive an AppendEntries message from a leader with current Term
type Follower struct {
	Term     int64
	VotedFor string
}

// if you're a candidate, the only thing that causes you to reset your election timer is when:
// * you win an election by getting enough votes (set it to never)
// * you lose an election by finding out about a term equal to or greater than yours (via AppendEntries OR RequestVote)
// * you time out and start a new election
type Candidate struct {
	Term        int64
	votesNeeded int64
	RunningState
}

func (l *Leader) GetTerm() int64 {
	return l.Term
}

// This signature is a little nasty
func WithVote(candidateId string) func(state *Follower, result *EventResult) (*Follower, *EventResult) {
	return func(state *Follower, result *EventResult) (*Follower, *EventResult) {
		state.VotedFor = candidateId
		result.Success = true
		return state, result
	}
}

func WithSuccess(state *Follower, result *EventResult) (*Follower, *EventResult) {
	result.Success = true
	return state, result
}

func WithError(err error) func(state *Follower, result *EventResult) (*Follower, *EventResult) {
	return func(state *Follower, result *EventResult) (*Follower, *EventResult) {
		result.Err = err
		return state, result
	}
}

func StartTermAsFollower(event StateEvent) (*Follower, *EventResult) {
	return &Follower{
			Term: event.GetTerm(),
		}, &EventResult{
			Term: event.GetTerm(),
		}
}

// NoOp is kind of interesting I think because it indicates that nothing happened as a RESULT of your event
// but it also communicates what has happened since your event was enqueued (the term could have incremented due to)
// another event that was processed before yours was
func NoOp(state State) (State, *EventResult) {
	return state, &EventResult{
		Term: state.GetTerm(),
	}
}

func StartTermAsCandidate(state State, votesNeeded int64) (State, *EventResult) {
	return &Candidate{
		Term:        state.GetTerm() + 1,
		votesNeeded: votesNeeded,
	}, nil
}

func BecomeTermLeader(state *Candidate) (State, *EventResult) {
	return &Leader{
		Term: state.GetTerm(),
	}, nil // I don't think anyone needs to be informed when this happens
}

func (l *Leader) Apply(ctx context.Context, event StateEvent, t *timer, votesNeeded int64, lg Log) (State, *EventResult) {
	// This is interesting, I accidentally called this with a *StateEvent and that causes
	// it to not match on the event type, which causes the function to ignore the event...
	switch event := event.(type) {
	case AppendEntriesRequest, AppendEntriesResponse:
		if l.Term < event.GetTerm() {
			l.Shutdown(ctx)
			t.Start()
			return WithSuccess(StartTermAsFollower(event))
		}
	case RequestVoteRequest:
		if l.Term < event.GetTerm() {
			l.Shutdown(ctx)
			t.Start()
			return WithVote(event.CandidateId)(StartTermAsFollower(event))
		}
	case ElectionTimeout:
		// TODO: remove this when I'm confident this all works. We should not
		// ever start the election timer after we've begun a term as leader
		// See also: http://petertsehsun.github.io/soen691/current/papers/osdi14-paper-yuan.pdf
		panic("cannot election timeout after leader begins term")
		// case RequestVoteResponse:
		// this could be a vote from this election, but by the time we're leader we don't care
	}
	return NoOp(l)
}

func (l *Leader) Run(ctx context.Context, stateTaskManager StateTaskManager) {
	l.RunningState = stateTaskManager.StartTermAsLeader(ctx, l.GetTerm())
}

func (l *Leader) Shutdown(ctx context.Context) {
	l.Stop()
}

func (c *Follower) Run(ctx context.Context, stateTaskManager StateTaskManager) {
	// Follower is entirely passive
}

func (f *Follower) Apply(ctx context.Context, event StateEvent, t *timer, votesNeeded int64, lg Log) (State, *EventResult) {
	// Follower doesn't care about votes because they would have to be from a prior term
	switch event := event.(type) {
	// If we find out about a more recent term, we transition to a new Follower state
	case AppendEntriesRequest:
		if f.Term <= event.GetTerm() {
			t.Start()

			success, err := lg.AppendIfSafe(ctx, event.Entries, event.PrevLogIndex, event.PrevLogTerm)
			// TODO: how to handle this error
			if err != nil {
				return WithError(err)(StartTermAsFollower(event))
			}
			if success {
				return WithSuccess(StartTermAsFollower(event))
			}

			//success, err := s.log.AppendIfSafe(in.GetEntries(), in.GetPrevLogIndex(), in.GetPrevLogTerm())
			//if err != nil {
			//	return nil, fmt.Errorf("appending entries to log: %v", err)
			//}
			return StartTermAsFollower(event)
		}
	case AppendEntriesResponse:
		// if this is for our term or a greater one, we'll accept it
		// if this is starting a new term, we should update our term
		if f.Term <= event.GetTerm() {
			// restart our election timeout
			t.Start()
			return StartTermAsFollower(event)
		}
	case RequestVoteRequest:
		// If this vote request is for this term, reissue vote if I've already voted for this candidate
		if f.Term == event.GetTerm() && (f.VotedFor == event.CandidateId) {
			return WithVote(event.CandidateId)(f, &EventResult{Term: f.Term})
		}
		// If this is a vote request for a new term, I'll start that term and vote for this candidate
		if f.Term < event.GetTerm() {
			return WithVote(event.CandidateId)(StartTermAsFollower(event))
		}
		// Otherwise, I've already voted for this term for someone else or this is an old term
	case ElectionTimeout:
		t.Start()
		return StartTermAsCandidate(f, votesNeeded)
	}
	return NoOp(f)
}

func (f *Follower) Shutdown(ctx context.Context) {
	// No-op: follower doesn't do anything
}

func (f *Follower) GetTerm() int64 {
	return f.Term
}

func (c *Candidate) AchievedQuorum() bool {
	return c.votesNeeded <= 0 // math.Ceil(float64(cluster.clients / 2))
}

func (c *Candidate) Run(ctx context.Context, stateTaskManager StateTaskManager) {
	c.RunningState = stateTaskManager.StartElection(ctx)
}

func (c *Candidate) Apply(ctx context.Context, event StateEvent, t *timer, votesNeeded int64, lg Log) (State, *EventResult) {
	switch event := event.(type) {
	// we respect a response from a prior term as leader if it's late and relevant
	case AppendEntriesRequest:
		// TODO: reduce repetition in here with Follower
		if c.Term <= event.GetTerm() {
			c.Shutdown(ctx)
			t.Start()

			success, err := lg.AppendIfSafe(ctx, event.Entries, event.PrevLogIndex, event.PrevLogTerm)
			// TODO: how to handle this error
			if err != nil {
				return WithError(err)(StartTermAsFollower(event))
			}
			if success {
				return WithSuccess(StartTermAsFollower(event))
			}
			return StartTermAsFollower(event)
		}
	case AppendEntriesResponse:
		if c.Term <= event.GetTerm() {
			c.Shutdown(ctx)
			t.Start()
			log.Println("candidate -> follower:", event.GetTerm())
			return WithSuccess(StartTermAsFollower(event))
		}
	// Also respect a RequestVoteRequest from another Candidate at a higher term
	case RequestVoteRequest:
		log.Println("Received vote request as a candidate")
		if c.Term < event.GetTerm() {
			c.Shutdown(ctx)
			t.Start()
			log.Println("candidate -> follower:", event.GetTerm())
			return WithVote(event.CandidateId)(StartTermAsFollower(event))
		}
	case RequestVoteResponse:
		if c.Term < event.Term {
			c.Shutdown(ctx)
			t.Start()
			return StartTermAsFollower(event)
		}
		// If this is a response from a prior term, ignore it, continue with election
		if c.Term > event.Term {
			return NoOp(c)
		}
		if event.VoteGranted {
			c.votesNeeded--
			if c.AchievedQuorum() {
				// Leader does not have an ElectionTimeout
				t.Pause()
				c.Shutdown(ctx)
				return BecomeTermLeader(c)
			}
		}
	case ElectionTimeout:
		// Start a new election
		c.Shutdown(ctx)
		t.Start()
		return StartTermAsCandidate(c, votesNeeded)
	}
	return NoOp(c)
}

func (c *Candidate) Shutdown(ctx context.Context) {
	c.Stop()
}

func (c *Candidate) GetTerm() int64 {
	return c.Term
}
