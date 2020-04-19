package paxos

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//


// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

// use for test
var id int = 1000000

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

/*
the Paxos pseudo-code (for a single instance) from the lecture:

proposer(v):
	while not decided:
	choose n, unique and higher than any n seen so far
	send prepare(n) to all servers including self
	if prepare_ok(n, n_a, v_a) from majority:
		v' = v_a with highest n_a; choose own v otherwise
		send accept(n, v') to all
		if accept_ok(n) from majority:
			send decided(v') to all

acceptor's state:
	n_p (highest prepare seen)
	n_a, v_a (highest accept seen)

acceptor's prepare(n) handler:
	if n > n_p
		n_p = n
		reply prepare_ok(n, n_a, v_a)
	else
		reply prepare_reject

acceptor's accept(n, v) handler:
	if n >= n_p
		n_p = n
		n_a = n
		v_a = v
		reply accept_ok(n)
	else
		reply accept_reject
*/

type InstanceState struct {
	instance                 *interface{}
	state                    Fate
	seq                      int
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	n_p                         int
	v_p                         interface{}
	n_a                         int
	v_a                         interface{}
	prepareVote                 *PrepareReply
	prepareVoteCounter          int
	prepared                    bool
	accepted                    bool
	acceptVoteCounter           int
	instanceState               map[int]*InstanceState

	prepareReplyChan            chan *PrepareExt
	prepareArgsChan             chan *PrepareArgs
	prepareReplyInterChan       chan *PrepareReply
	acceptReplyChan             chan *AcceptExt
	acceptArgsChan              chan *AcceptArgs
	acceptReplyInterChan        chan *AcceptReply
	decidedReplyChan            chan *DecidedExt
	decidedArgsChan             chan *DecidedArgs
	decidedReplyInterChan       chan *DecidedReply
	exitChan                    chan bool

	timer                       *time.Timer

	id                          int
	debug                       bool
}

func (px *Paxos) dump(prefix string, debug bool) {
	if debug == false {
		return
	}
	dumpLog := fmt.Sprintf(" paxos: %d, %s, paxos state: \n", px.id, prefix)
	log.Printf(dumpLog)
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

type PrepareArgs struct {
	N       int
	V       interface{}
}

type PrepareReply struct {
	N        int
	N_a      int
	V_a      interface{}
}

type PrepareExt struct {
	Args      *PrepareArgs
	Reply     *PrepareReply
}

func (args *PrepareArgs) dump(debug bool, id int) {
	if debug == false {
		return
	}
	dumpLog := fmt.Sprintf(" paxos: %d, PrepareArgs, Seq: %d", id, args.N)
	log.Printf(dumpLog)
}

func (reply *PrepareReply) dump(debug bool, id int) {
	if debug == false {
		return
	}
	dumpLog := fmt.Sprintf(" paxos: %d, PrepareReply, Seq: %d", id, reply.N)
	log.Printf(dumpLog)
}

func (px *Paxos) Prepare(n int, v interface{}) {
	px.mu.Lock()
	px.n_p = n
	px.v_p = v
	args := &PrepareArgs{
		N: n,
		V: v,
	}
	px.mu.Unlock()
	for _, peer := range px.peers {
		go func(server string) {
			var reply PrepareReply
			call(server, "Paxos.PrepareVote", args, &reply)
			ext := &PrepareExt{
				Args: args,
				Reply: &reply,
			}
			px.prepareReplyChan <- ext
		}(peer)
	}
}

func (px *Paxos) PrepareVote(args *PrepareArgs, reply *PrepareReply) {
	px.prepareArgsChan <- args
	replyInternal, ok := <- px.prepareReplyInterChan
	if !ok || replyInternal == nil {
		log.Fatal("PrepareVote fatal.")
	} else {
		*reply = *replyInternal
	}
}


type AcceptArgs struct {
	N          int
	V          interface{}
}

type AcceptReply struct {
	N          int
}

type AcceptExt struct {
	Args      *AcceptArgs
	Reply     *AcceptReply
}

func (args *AcceptArgs) dump(debug bool, id int) {
	if debug == false {
		return
	}
	dumpLog := fmt.Sprintf(" paxos: %d, AcceptArgs, Seq: %d", id, args.N)
	log.Printf(dumpLog)
}

func (reply *AcceptReply) dump(debug bool, id int) {
	if debug == false {
		return
	}
	dumpLog := fmt.Sprintf(" paxos: %d, AcceptReply, Seq: %d", id, reply.N)
	log.Printf(dumpLog)
}

func (px *Paxos) Accept(n int, v interface{}) {
	args := &AcceptArgs{
		N: n,
		V: v,
	}
	for _, peer := range px.peers {
		go func(server string) {
			var reply AcceptReply
			call(server, "Paxos.AcceptVote", args, &reply)
			ext := &AcceptExt{
				Args: args,
				Reply: &reply,
			}
			px.acceptReplyChan <- ext
		}(peer)
	}
}

func (px *Paxos) AcceptVote(args *AcceptArgs, reply *AcceptReply) {
	px.acceptArgsChan <- args
	replyInternal, ok := <- px.acceptReplyInterChan
	if !ok || replyInternal == nil {
		log.Fatal("AcceptVote fatal.")
	} else {
		*reply = *replyInternal
	}
}

type DecidedArgs struct {
	N          int
	V          interface{}
}

type DecidedReply struct {
}

type DecidedExt struct {
	Args      *DecidedArgs
	Reply     *DecidedReply
}

func (args *DecidedArgs) dump(debug bool, id int) {
	if debug == false {
		return
	}
	dumpLog := fmt.Sprintf(" paxos: %d, DecidedArgs, ", id)
	log.Printf(dumpLog)
}

func (reply *DecidedReply) dump(debug bool, id int) {
	if debug == false {
		return
	}
	dumpLog := fmt.Sprintf(" paxos: %d, DecidedReply, ", id)
	log.Printf(dumpLog)
}

func (px *Paxos) Decided(n int, v interface{}) {
	args := &DecidedArgs{
		N: n,
		V: v,
	}
	for _, peer := range px.peers {
		go func(server string) {
			var reply DecidedReply
			call(server, "Paxos.DecidedReceive", args, &reply)
			ext := &DecidedExt{
				Args: args,
				Reply: &reply,
			}
			px.decidedReplyChan <- ext
		}(peer)
	}
}

func (px *Paxos) DecidedReceive(args *DecidedArgs, reply *DecidedReply) {
	px.decidedArgsChan <- args
	replyInternal, ok := <- px.decidedReplyInterChan
	if !ok || replyInternal == nil {
		log.Fatal("DecidedReceive fatal.")
	} else {
		*reply = *replyInternal
	}
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	px.Prepare(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	for k, v := range px.instanceState {
		if k <= seq {
			v.state = Forgotten
		}
	}
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	max := 0
	for k, v := range px.instanceState {
		if v.state == Decided && k > max {
			max = k
		}
	}
	return max
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	min := math.MaxInt32
	for k, v := range px.instanceState {
		if v.state == Decided && k < min {
			min = k
		}
	}
	return min
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	state, ok := px.instanceState[seq]
	if !ok {
		return Forgotten, nil
	}
	return state.state, nil
}



//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
	px.exitChan <- true
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me
	px.id = id
	id ++
	// Your initialization code here.
	px.prepareReplyChan = make(chan *PrepareExt)
	px.prepareArgsChan = make(chan *PrepareArgs)
	px.prepareReplyInterChan = make(chan *PrepareReply)
	px.acceptReplyChan = make(chan *AcceptExt)
	px.acceptArgsChan = make(chan *AcceptArgs)
	px.acceptReplyInterChan = make(chan *AcceptReply)
	px.decidedReplyChan = make(chan *DecidedExt)
	px.decidedArgsChan = make(chan *DecidedArgs)
	px.decidedReplyInterChan = make(chan *DecidedReply)
	px.exitChan = make(chan bool)

	go px.eventLoop()

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}

func (px *Paxos) handlePrepareVote(args *PrepareArgs) *PrepareReply {
	args.dump(px.debug, px.id)
	px.dump("Before handlePrepareVote", px.debug)
	var reply PrepareReply
	state := &InstanceState{
		instance: &args.V,
		seq: args.N,
		state: Pending,
	}
	px.instanceState[args.N] = state
	if args.N > px.n_p {
		px.n_p = args.N
		px.prepareVoteCounter = 0
		px.prepared = false
		px.prepareVote = nil
		reply.N = args.N
		reply.N_a = px.n_a
		reply.V_a = px.v_a
	} else {
		reply.N = args.N
		reply.N_a = -1
	}
	return &reply
}

func (px *Paxos) handlePrepareReply(ext *PrepareExt) {
	ext.Reply.dump(px.debug, px.id)
	px.dump("Before handlePrepareReply", px.debug)
	reply := ext.Reply
	if reply.N_a == -1 {
		return
	}
	if reply.N != px.n_p {
		return
	}
	if px.prepared == true {
		return
	}
	if px.prepareVote == nil {
		px.prepareVote = reply
	} else if reply.N_a > px.prepareVote.N_a {
		px.prepareVote = reply
	}
	px.prepareVoteCounter ++
	if px.prepareVoteCounter > len(px.peers) / 2 {
		px.prepared = true
		if px.prepareVote.N_a > px.n_a {
			px.n_p = px.prepareVote.N_a
			px.v_p = px.prepareVote.V_a
		}
		px.Accept(px.n_p, px.v_p)
	}
}

func (px *Paxos) handleAcceptVote(args *AcceptArgs) *AcceptReply {
	args.dump(px.debug, px.id)
	px.dump("Before handleAcceptVote", px.debug)
	var reply AcceptReply
	if args.N >= px.n_p {
		px.n_p = args.N
		px.n_a = args.N
		px.v_a = args.V
		px.acceptVoteCounter = 0
		px.accepted = false
		reply.N = args.N
	} else {
		reply.N = -1
	}
	return &reply
}

func (px *Paxos) handleAcceptReply(ext *AcceptExt) {
	ext.Reply.dump(px.debug, px.id)
	px.dump("Before handleAcceptReply", px.debug)
	reply := ext.Reply
	if reply.N == -1 {
		return
	}
	if reply.N != px.n_a {
		return
	}
	if px.accepted == true {
		return
	}
	px.acceptVoteCounter ++
	if px.acceptVoteCounter > len(px.peers)/2 {
		px.Decided(px.n_a, px.v_a)
	}
}

func (px *Paxos) handleDecided(args *DecidedArgs) *DecidedReply {
	args.dump(px.debug, px.id)
	px.dump("Before handleDecided", px.debug)
	var reply DecidedReply
	px.instanceState[args.N].state = Decided
	return &reply
}

func (px *Paxos) handleDecidedReply(ext *DecidedExt) {
	ext.Reply.dump(px.debug, px.id)
	px.dump("Before handleDecidedReply", px.debug)
}

func (px *Paxos) eventLoop() {
	for {
		select {
		case <- px.timer.C:

		case prepareArgs, ok :=  <- px.prepareArgsChan:
			if !ok || prepareArgs == nil {
				break
			}
			reply := px.handlePrepareVote(prepareArgs)
			px.prepareReplyInterChan <- reply
		case prepareReply, ok := <- px.prepareReplyChan:
			if !ok || prepareReply == nil {
				break
			}
			px.handlePrepareReply(prepareReply)
		case acceptArgs, ok := <- px.acceptArgsChan:
			if !ok || acceptArgs == nil {
				break
			}
			reply := px.handleAcceptVote(acceptArgs)
			px.acceptReplyInterChan <- reply
		case acceptReply, ok := <- px.acceptReplyChan:
			if !ok || acceptReply == nil {
				break
			}
			px.handleAcceptReply(acceptReply)
		case decidedArgs, ok := <- px.decidedArgsChan:
			if !ok || decidedArgs == nil {
				break
			}
			reply := px.handleDecided(decidedArgs)
			px.decidedReplyInterChan <- reply
		case decidedReply, ok := <- px.decidedReplyChan:
			if !ok || decidedReply == nil {
				break
			}
			px.handleDecidedReply(decidedReply)
		case exit, ok := <- px.exitChan:
			if !ok || exit != true {
				break
			}
			px.timer.Stop()
			return
		}
	}
}
