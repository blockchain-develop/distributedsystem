package pbservice

import (
	"net"
)
import "fmt"
import "net/rpc"
import "log"
import "time"
import "../viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

type RequestState struct {
	number         int
	state          int
}

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk
	// Your declarations here.
	view       *viewservice.View
	data       map[string]string
	state      int
	synced     bool
	partition  bool
	requests   map[string]*RequestState
	debug      bool
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	args.dump(pb.me, pb.debug)
	if pb.view.Primary != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}
	if pb.state != CONFIRM_PRIMARY {
		reply.Err = ErrWrongServer
		return nil
	}
	if pb.partition == true {
		reply.Err = ErrWrongServer
		return nil
	}
	v,ok := pb.data[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		reply.Value = ""
	} else {
		reply.Err = OK
		reply.Value = v
	}
	reply.dump(pb.me, pb.debug)
	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	args.dump(pb.me, "", pb.debug)
	err, result := pb.requestState(args)
	if err != nil{
		return err
	}
	if result == OK {
		reply.Err = result
		return nil
	}
	for true {
		pb.mu.Lock()
		isPrimary := (pb.view.Primary == pb.me)
		backup := pb.view.Backup
		if !isPrimary {
			reply.Err = ErrWrongServer
			pb.mu.Unlock()
			return nil
		}
		if pb.state != CONFIRM_PRIMARY {
			reply.Err = ErrWrongServer
			pb.mu.Unlock()
			return nil
		}
		if pb.partition == true {
			reply.Err = ErrWrongServer
			pb.mu.Unlock()
			return nil
		}
		reqeustState := pb.requests[args.From]
		reqeustState.number = args.Number
		reqeustState.state = HANDLING
		if backup == "" {
			pb.acceptValue(args.Key, args.Value, args.Op)
			reply.Err = OK
			reqeustState.state = HANDLED
			pb.mu.Unlock()
			return nil
		}
		syncReply := pb.syncPutAppend(pb.view.Backup, args)
		if syncReply != nil && syncReply.Err == OK {
			pb.acceptValue(args.Key, args.Value, args.Op)
			reply.Err = OK
			reqeustState.state = HANDLED
			pb.mu.Unlock()
			return nil
		}
		pb.mu.Unlock()
		time.Sleep(time.Second * 1)
	}
	return fmt.Errorf("this not happen.")
}

func (pb *PBServer) PutAppendSync(args *PutAppendArgs, reply *PutAppendReply) error {
	args.dump(pb.me, "Sync", pb.debug)
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.view.Backup != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}
	pb.acceptValue(args.Key, args.Value, args.Op)
	reply.Err = OK
	return nil
}

func (pb *PBServer) CopySync(args *CopyArgs, reply *CopyReply) error {
	args.dump(pb.me, pb.debug)
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.view.Backup != pb.me {
		reply.Err = ErrWrongServer
		return nil
	}
	pb.data = args.Data
	pb.synced = true
	reply.Err = OK
	return nil
}

func (pb *PBServer) syncPutAppend(node string, args *PutAppendArgs) *PutAppendReply {
	var reply PutAppendReply
	// send an RPC request, wait for the reply.
	ok := call(node, "PBServer.PutAppendSync", args, &reply)
	if ok == false {
		return nil
	}
	return &reply
}

func (pb *PBServer) syncCopy(node string) *CopyReply {
	args := &CopyArgs{}
	args.Data = pb.data
	var reply CopyReply
	// send an RPC request, wait for the reply.
	ok := call(node, "PBServer.CopySync", args, &reply)
	if ok == false {
		return nil
	}
	return &reply
}

func (pb *PBServer) acceptValue(key string, value string, op string) {
	newValue := ""
	if op == "Put" {
		newValue = value
		pb.data[key] = value
	} else if op == "Append" {
		v, ok := pb.data[key]
		if ok {
			newValue = v + value
		} else {
			newValue = value
		}
	}
	pb.data[key] = newValue
	if pb.debug == true {
		log.Printf(" PBServer, acceptValue, %s, key: %s, value: %s", pb.me, key, newValue)
	}
}

func (pb *PBServer) requestState(args *PutAppendArgs) (error, Err) {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	state, ok := pb.requests[args.From]
	if !ok {
		state = &RequestState{
			number: -1,
			state: 0,
		}
		pb.requests[args.From] = state
	}
	if state.number > args.Number {
		return nil, OK
	}
	if state.state == HANDLING {
		return fmt.Errorf("request is handling."), Handling
	}
	if state.number == args.Number {
		return nil, OK
	}
	return nil, ""
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	pb.dumpState("Tick")
	viewnum := uint(0)
	if pb.view != nil {
		viewnum = pb.view.Viewnum
	}
	if pb.state == ASSIGN_PRIMARY && pb.synced == false {
		return
	}
	newView, err := pb.vs.Ping(viewnum)
	if err != nil {
		pb.partition = true
		return
	}
	pb.partition = false
	if newView.Primary == pb.me {
		if pb.view == nil || pb.view.Primary != pb.me {
			pb.state = ASSIGN_PRIMARY
			pb.view = &newView
			// first boot
			if newView.Viewnum == 1 {
				pb.synced = true
			}
			return
		}
		if pb.view.Primary == pb.me {
			if pb.state == ASSIGN_PRIMARY {
				pb.synced = false
				pb.requests = make(map[string]*RequestState, 0)
				pb.state = CONFIRM_PRIMARY
			}
			if newView.Viewnum > pb.view.Viewnum {
				pb.synced = false
			}
			if newView.Backup != pb.view.Backup {
				pb.synced = false
			}
			if pb.synced == false {
				if newView.Backup != "" {
					reply := pb.syncCopy(newView.Backup)
					if reply != nil && reply.Err == OK {
						pb.synced = true
					}
				}
			}
			pb.view = &newView
			return
		}
	}
	pb.view = &newView
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

// call this to find out if the server is dead.
func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func (pb *PBServer) dumpState(prefix string) {
	if pb.debug == false {
		return
	}
	dumpLog := fmt.Sprintf(" PBServer state, %s, %s: \n", prefix, pb.me)
	if pb.view != nil {
		dumpLog += fmt.Sprintf(" latest view, view num: %d, primary: %s, backup: %s\n", pb.view.Viewnum, pb.view.Primary, pb.view.Backup)
	}
	dumpLog += fmt.Sprintf(" server state: %d\n", pb.state)
	dumpLog += fmt.Sprintf(" server synced: %v\n", pb.synced)
	log.Printf(dumpLog)
}

func (args *PutAppendArgs) dump(me string, prefix string, debug bool) {
	if debug == false {
		return
	}
	log.Printf(" PBServer, PutAppendArgs, %s, %s, from: %s, number: %d, key: %s, value: %s, op: %s", prefix, me, args.From, args.Number, args.Key, args.Value, args.Op)
}
func (reply *PutAppendReply) dump(me string, prefix string, debug bool) {
	if debug == false {
		return
	}
	log.Printf(" PBServer, PutAppendReply, %s, %s, result: %s", prefix, me, reply.Err)
}


func (args *CopyArgs) dump(me string, debug bool) {
	if debug == false {
		return
	}
	log.Printf(" PBServer, CopyArgs, Sync, %s", me)
}

func (args *GetArgs) dump(me string, debug bool) {
	if debug == false {
		return
	}
	log.Printf(" PBServer, GetArgs, %s, key: %s", me, args.Key)
}

func (reply *GetReply) dump(me string, debug bool) {
	if debug == false {
		return
	}
	log.Printf(" PBServer, GetReply, %s, result: %s, value: %s", me, reply.Err, reply.Value)
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// Your pb.* initializations here.
	pb.data = make(map[string]string, 0)
	pb.view = nil
	pb.state = IDLE
	pb.synced = false
	pb.debug = true
	pb.requests = make(map[string]*RequestState)
	pb.partition = false

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.
	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
