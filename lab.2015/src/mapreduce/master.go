package mapreduce

import "container/list"
import "fmt"


type WorkerInfo struct {
	address string
	// You can add definitions here.
	state   int
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) assignWork(worker string, jobNumber int, jobType JobType, number int) {
	args := &DoJobArgs{}
	args.File = mr.file
	args.Operation = jobType
	args.JobNumber = jobNumber
	args.NumOtherPhase = number
	var reply DoJobReply
	ok := call(worker, "Worker.DoJob", args, &reply)
	mr.jobChan <- &DoJobExt{
		Worker: worker,
		Args: args,
		Reply: &reply,
	}
	if ok == false {
		fmt.Printf("assign work to Woker: %s error\n", worker)
	}
}

func (mr *MapReduce) getIdleWoker() *WorkerInfo {
	for _, worker := range mr.Workers {
		if worker.state == 0 {
			return worker
		}
	}
	return nil
}

func (mr *MapReduce) getUnhandleMapWork() int {
	for mapwork, workstate := range mr.MapWorkState {
		if workstate == 0 {
			return mapwork
		}
	}
	return -1
}

func (mr *MapReduce) mapWorkFinished() bool {
	for _, workstate := range mr.MapWorkState {
		if workstate != 2 {
			return false
		}
	}
	return true
}
func (mr *MapReduce) getUnhandleReduceWork() int {
	for reducework, workstate := range mr.ReduceWorkState {
		if workstate == 0 {
			return reducework
		}
	}
	return -1
}

func (mr *MapReduce) reduceWorkFinished() bool {
	for _, workstate := range mr.ReduceWorkState {
		if workstate != 2 {
			return false
		}
	}
	return true
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	finished := false
	for {
		for true {
			unhandleMapWork := mr.getUnhandleMapWork()
			if unhandleMapWork != -1 {
				idleWoker := mr.getIdleWoker()
				if idleWoker != nil {
					mr.MapWorkState[unhandleMapWork] = 1
					idleWoker.state = 1
					go mr.assignWork(idleWoker.address, unhandleMapWork, Map, mr.nReduce)
					continue
				} else {
					break
				}
			}
			mapWorkFinished := mr.mapWorkFinished()
			if mapWorkFinished == false {
				break
			}
			unhandleReduceWork := mr.getUnhandleReduceWork()
			if unhandleReduceWork != -1 {
				idleWorker := mr.getIdleWoker()
				if idleWorker != nil {
					mr.ReduceWorkState[unhandleReduceWork] = 1
					idleWorker.state = 1
					go mr.assignWork(idleWorker.address, unhandleReduceWork, Reduce, mr.nMap)
					continue
				} else {
					break
				}
			}
			reduceWorkFinished := mr.reduceWorkFinished()
			if reduceWorkFinished == true {
				finished = true
			}
			break
		}
		if finished == true {
			break
		}
		select {
		case worker, ok := <- mr.registerChannel:
			if !ok {
				break
			}
			mr.Workers[worker] = &WorkerInfo{
				address: worker,
				state: 0,
			}
		case doJobExt, ok := <- mr.jobChan:
			if !ok {
				break
			}
			mr.Workers[doJobExt.Worker].state = 0
			if doJobExt.Reply.OK == false {
				if doJobExt.Args.Operation == Map {
					mr.MapWorkState[doJobExt.Args.JobNumber] = 0
				} else {
					mr.ReduceWorkState[doJobExt.Args.JobNumber] = 0
				}
			} else {
				if doJobExt.Args.Operation == Map {
					mr.MapWorkState[doJobExt.Args.JobNumber] = 2
				} else {
					mr.ReduceWorkState[doJobExt.Args.JobNumber] = 2
				}
			}
		}
	}
	return mr.KillWorkers()
}
