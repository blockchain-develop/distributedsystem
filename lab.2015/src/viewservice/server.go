package viewservice

import (
	"net"
)
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

const (
	NO_PRIMARY          = iota
	ASSIGN_PRIMARY
	CONFIRM_PRIMARY
)

type ViewServer struct {
	mu           sync.Mutex
	l            net.Listener
	dead         int32 // for testing
	rpccount     int32 // for testing
	me           string

	// Your declarations here.
	views        []*View
	pings        map[string]int64
	state        int
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	vs.dumpState("Before Ping")
	args.dump()
	vs.pings[args.Me] = time.Now().Unix()
	if len(vs.views) == 0 {
		vs.views = append(vs.views, &View{
			Viewnum: 1,
			Primary: args.Me,
			Backup: "",
		})
		vs.state = ASSIGN_PRIMARY
		reply.View = *(vs.views[len(vs.views) - 1])
		reply.dump()
		return nil
	}

	view := vs.views[len(vs.views) - 1]
	if view.Primary == "" {
		vs.views = append(vs.views, &View{
			Viewnum: view.Viewnum + 1,
			Primary: args.Me,
			Backup: view.Backup,
		})
		vs.state = ASSIGN_PRIMARY
		reply.View = *(vs.views[len(vs.views) - 1])
		reply.dump()
		return nil
	}
	if args.Me == view.Primary {
		if vs.state == ASSIGN_PRIMARY {
			vs.views = append(vs.views, &View{
				Viewnum: view.Viewnum + 1,
				Primary: view.Primary,
				Backup:  view.Backup,
			})
			vs.state = CONFIRM_PRIMARY
			reply.View = *(vs.views[len(vs.views)-1])
			reply.dump()
			return nil
		} else {
			reply.View = *(vs.views[len(vs.views)-1])
			reply.dump()
			return nil
		}
	} else if args.Me == view.Backup {
		reply.View = *(vs.views[len(vs.views)-1])
		reply.dump()
		return nil
	}
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.

	return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	vs.dumpState("Before Tick")
	if len(vs.views) == 0 {
		return
	}
	current := time.Now().Unix()
	view := vs.views[len(vs.views) - 1]
	for k,v := range vs.pings {
		if current - v > DeadPings {
			if k == view.Primary {
				vs.views = append(vs.views, &View{
					Viewnum: view.Viewnum + 1,
					Primary: view.Backup,
					Backup: "",
				})
				vs.state = ASSIGN_PRIMARY
			} else if k == view.Backup {
				vs.views = append(vs.views, &View{
					Viewnum: view.Viewnum + 1,
					Primary: view.Primary,
					Backup: "",
				})
			}
		}
	}
	vs.dumpState("After Tick")
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func (vs *ViewServer) dumpState(prefix string) {
	dumpLog := fmt.Sprintf(" %s, view server state: \n")
	if len(vs.views) != 0 {
		view := vs.views[len(vs.views) - 1]
		dumpLog += fmt.Sprintf(" latest view, view num: %d, primary: %s, backup: %s\n", view.Viewnum, view.Primary, view.Backup)
	}
	dumpLog += fmt.Sprintf(" view server state: %d\n", vs.state)
	current := time.Now().Unix()
	pingState := fmt.Sprintf(" current time: %d\n [", current)
	for k,v := range vs.pings {
		pingState += fmt.Sprintf(" %s - %d,%d ", k, v, current -v)
	}
	pingState += " ]"
	log.Printf(pingState)
}

func (args *PingArgs) dump() {
	log.Printf(" PingArgs, view num: %d, node: %s", args.Viewnum, args.Me)
}

func (reply *PingReply) dump() {
	log.Printf(" PingReply, view num: %d, primary: %s, backup: %s", reply.View.Viewnum, reply.View.Primary, reply.View.Backup)
}

func (view *View) dump() {
	log.Printf(" View, view num: %d, primary: %s, backup: %s", view.Viewnum, view.Primary, view.Backup)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.views = make([]*View, 0)
	vs.pings = make(map[string]int64, 0)
	vs.state = NO_PRIMARY

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
