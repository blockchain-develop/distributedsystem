package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	mapHandle         map[string]*MapHandleInfo
	reduceHandle      map[int]*ReduceHandleInfo
	nReduce           int
	nMap              int
}

// Your code here -- RPC handlers for the worker to call.
const (
	HANDLE_INIT = iota
	HANDLING
	HANDLE_FINISHED
)

type MapHandleInfo struct {
	StartTime         int64
	EndTime           int64
	WorkerId          int
	State             int
	MapNumber         int
}

type ReduceHandleInfo struct {
	StartTime         int64
	EndTime           int64
	WorkerId          int
	State             int
	ReduceNumber      int
}

func (m *Master) Work(req *WorkRequest, rsp *WorkResponse) error {
	// update handle state if worker finished work
	if req.WorkInd == WORK_MAP {
		handleInfo := m.mapHandle[req.WorkKey]
		handleInfo.State = HANDLE_FINISHED
		handleInfo.EndTime = time.Now().Unix()
	} else if req.WorkInd == WORK_REDUCE {
		handleInfo := m.reduceHandle[req.WorkNumber]
		handleInfo.State = HANDLE_FINISHED
		handleInfo.EndTime = time.Now().Unix()
	}
	// assigment new map work
	for fileName, handleInfo := range m.mapHandle {
		if handleInfo.State == HANDLE_INIT {
			handleInfo.State = HANDLING
			handleInfo.StartTime = time.Now().Unix()
			handleInfo.WorkerId = req.WorkerId
			rsp.WorkInd = WORK_MAP
			rsp.WorkKey = fileName
			rsp.WorkNumber = handleInfo.MapNumber
			rsp.Number = m.nReduce
			return nil
		}
	}
	for fileName, handleInfo := range m.mapHandle {
		if handleInfo.State == HANDLING && time.Now().Unix() - handleInfo.StartTime > 10 {
			handleInfo.State = HANDLING
			handleInfo.StartTime = time.Now().Unix()
			handleInfo.WorkerId = req.WorkerId
			rsp.WorkInd = WORK_MAP
			rsp.WorkKey = fileName
			rsp.WorkNumber = handleInfo.MapNumber
			rsp.Number = m.nReduce
			return nil
		}
	}
	for _, handleInfo := range m.mapHandle {
		if handleInfo.State == HANDLING {
			rsp.WorkInd = WORK_WAITING
			return nil
		}
	}
	// assigment new reduce work
	for reduceNumber, handleInfo := range m.reduceHandle {
		if handleInfo.State == HANDLE_INIT {
			handleInfo.State = HANDLING
			handleInfo.StartTime = time.Now().Unix()
			handleInfo.WorkerId = req.WorkerId
			rsp.WorkInd = WORK_REDUCE
			rsp.WorkKey = ""
			rsp.WorkNumber = reduceNumber
			rsp.Number = m.nMap
			return nil
		}
	}
	for reduceNumber, handleInfo := range m.reduceHandle {
		if handleInfo.State == HANDLING && time.Now().Unix() - handleInfo.StartTime > 10 {
			handleInfo.State = HANDLING
			handleInfo.StartTime = time.Now().Unix()
			handleInfo.WorkerId = req.WorkerId
			rsp.WorkInd = WORK_REDUCE
			rsp.WorkKey = ""
			rsp.WorkNumber = reduceNumber
			rsp.Number = m.nMap
			return nil
		}
	}
	for _, handleInfo := range m.reduceHandle {
		if handleInfo.State == HANDLING {
			rsp.WorkInd = WORK_WAITING
			return nil
		}
	}
	rsp.WorkInd = WORK_NONE
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	ret = true
	for _, handleInfo := range m.mapHandle {
		if handleInfo.State != 2 {
			ret = false
			break
		}
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	mapHandle := make(map[string]*MapHandleInfo, 0)
	for i, fileName := range files {
		mapHandle[fileName] = &MapHandleInfo {
			MapNumber: i,
			StartTime: 0,
			EndTime: 0,
			WorkerId: 0,
			State: HANDLE_INIT,
		}
	}
	reduceHandle := make(map[int]*ReduceHandleInfo, 0)
	for i := 0;i < nReduce;i ++ {
		reduceHandle[i] = &ReduceHandleInfo{
			ReduceNumber: i,
			StartTime: 0,
			EndTime: 0,
			WorkerId: 0,
			State: HANDLE_INIT,
		}
	}
	m.nReduce = nReduce
	m.nMap = len(mapHandle)

	m.server()
	return &m
}
