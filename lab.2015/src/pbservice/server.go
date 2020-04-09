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
	requests   map[string]int
}


func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	pb.mu.Lock()
	defer pb.mu.Unlock()
	v,ok := pb.data[args.Key]
	if !ok {
		reply.Err = ErrNoKey
		reply.Value = ""
	} else {
		reply.Err = OK
		reply.Value = v
	}
	return nil
}


func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	log.Printf(" PBServer, PutAppend, %s, from: %s, number: %d, key: %s, value: %s, op: %s", pb.me, args.From, args.Number, args.Key, args.Value, args.Op)
	for true {
		pb.mu.Lock()
		number, ok := pb.requests[args.From]
		if ok && number == args.Number {
			pb.mu.Unlock()
			return fmt.Errorf("duplate request.")
		}
		isPrimary := (pb.view.Primary == pb.me)
		backup := pb.view.Backup
		if !isPrimary {
			reply.Err = ErrWrongServer
			pb.mu.Unlock()
			return fmt.Errorf("i am not primary.")
		}
		if pb.state != CONFIRM_PRIMARY {
			reply.Err = ErrWrongServer
			pb.mu.Unlock()
			return fmt.Errorf("primary is not confirmed.")
		}
		pb.requests[args.From] = args.Number
		if backup == "" {
			pb.acceptValue(args.Key, args.Value, args.Op)
			reply.Err = OK
			pb.mu.Unlock()
			return nil
		}
		reply := pb.syncPutAppend(pb.view.Backup, args)
		if reply != nil && reply.Err == OK {
			pb.acceptValue(args.Key, args.Value, args.Op)
			reply.Err = OK
			pb.mu.Unlock()
			return nil
		}
		pb.mu.Unlock()
		time.Sleep(time.Second * 1)
	}
	return fmt.Errorf("this not happen.")
}

func (pb *PBServer) PutAppendSync(args *PutAppendArgs, reply *PutAppendReply) error {
	log.Printf(" PBServer, PutAppendSync, %s", pb.me)
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.view.Backup != pb.me {
		reply.Err = ErrWrongServer
		return fmt.Errorf("i am not backup.")
	}
	v := pb.data[args.Key]
	v += args.Value
	pb.data[args.Key] = v
	reply.Err = OK
	return nil
}

func (pb *PBServer) CopySync(args *CopyArgs, reply *CopyReply) error {
	log.Printf(" PBServer, CopySync, %s", pb.me)
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if pb.view.Backup != pb.me {
		reply.Err = ErrWrongServer
		return fmt.Errorf("i am not backup.")
	}
	pb.data = args.data
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
	args.data = pb.data
	var reply CopyReply
	// send an RPC request, wait for the reply.
	ok := call(node, "PBServer.CopySync", args, &reply)
	if ok == false {
		return nil
	}
	return &reply
}

func (pb *PBServer) acceptValue(key string, value string, op string) {
	if op == "Put" {
		pb.data[key] = value
	} else if op == "Append" {
		v := pb.data[key]
		v += value
		pb.data[key] = v
	}
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
		log.Printf("ping err: %s", err.Error())
		return
	}
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
				pb.requests = make(map[string]int, 0)
				pb.state = CONFIRM_PRIMARY
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
	dumpLog := fmt.Sprintf(" PBServer state, %s, %s: \n", prefix, pb.me)
	if pb.view != nil {
		dumpLog += fmt.Sprintf(" latest view, view num: %d, primary: %s, backup: %s\n", pb.view.Viewnum, pb.view.Primary, pb.view.Backup)
	}
	dumpLog += fmt.Sprintf(" server state: %d\n", pb.state)
	dumpLog += fmt.Sprintf(" server synced: %v\n", pb.synced)
	log.Printf(dumpLog)
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
	pb.requests = make(map[string]int)

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
