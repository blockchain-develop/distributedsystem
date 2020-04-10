package paxos

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

import (
	"net"
	"time"
)
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"


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

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]
	// Your data here.
	prepareReplyChan            chan *PrepareExt
	prepareArgsChan             chan *PrepareArgs
	prepareReplyInterChan       chan *PrepareReply
	acceptReplyChan             chan *AcceptExt
	acceptArgsChan              chan *AcceptArgs
	acceptReplyInterChan        chan *AcceptReply
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
	Seq       int
	V         interface{}
}

type PrepareReply struct {
	Seq        int
	Seq_a      int
	V_a        interface{}
}

type PrepareExt struct {
	Args      *PrepareArgs
	Reply     *PrepareReply
}

func (args *PrepareArgs) dump(debug bool, id int) {
	if debug == false {
		return
	}
	dumpLog := fmt.Sprintf(" paxos: %d, PrepareArgs, Seq: %d", id, args.Seq)
	log.Printf(dumpLog)
}

func (reply *PrepareReply) dump(debug bool, id int) {
	if debug == false {
		return
	}
	dumpLog := fmt.Sprintf(" paxos: %d, PrepareReply, Seq: %d", id, reply.Seq)
	log.Printf(dumpLog)
}

func (px *Paxos) Prepare(seq int, v interface{}) {
	for _, peer := range px.peers {
		go func(server string) {
			args := &PrepareArgs{
				Seq: seq,
				V: v,
			}
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
	Seq          int
	V            interface{}
}

type AcceptReply struct {
	Seq          int
}

type AcceptExt struct {
	Args      *AcceptArgs
	Reply     *AcceptReply
}

func (args *AcceptArgs) dump(debug bool, id int) {
	if debug == false {
		return
	}
	dumpLog := fmt.Sprintf(" paxos: %d, AcceptArgs, Seq: %d", id, args.Seq)
	log.Printf(dumpLog)
}

func (reply *AcceptReply) dump(debug bool, id int) {
	if debug == false {
		return
	}
	dumpLog := fmt.Sprintf(" paxos: %d, AcceptReply, Seq: %d", id, reply.Seq)
	log.Printf(dumpLog)
}

func (px *Paxos) Accept(seq int, v interface{}) {
	for _, peer := range px.peers {
		go func(server string) {
			args := &AcceptArgs{
				Seq: seq,
				V: v,
			}
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
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	return 0
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
	return 0
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
	return Pending, nil
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
	return nil
}

func (px *Paxos) handlePrepareReply(ext *PrepareExt) {
	ext.Reply.dump(px.debug, px.id)
	px.dump("Before handlePrepareReply", px.debug)
}

func (px *Paxos) handleAcceptVote(args *AcceptArgs) *AcceptReply {
	args.dump(px.debug, px.id)
	px.dump("Before handleAcceptVote", px.debug)
	return nil
}

func (px *Paxos) handleAcceptReply(ext *AcceptExt) {
	ext.Reply.dump(px.debug, px.id)
	px.dump("Before handleAcceptReply", px.debug)
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
		case exit, ok := <- px.exitChan:
			if !ok || exit != true {
				break
			}
			px.timer.Stop()
			return
		}
	}
}
