package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	req := WorkRequest{}
	req.WorkerId = 1
	req.WorkInd = WORK_NONE
	for true {
		rsp := WorkResponse{}
		call("Master.Work", &req, &rsp)
		if rsp.WorkInd == WORK_MAP {
			MapWork(mapf, rsp.WorkKey, rsp.WorkNumber, rsp.Number)
			req.WorkInd = WORK_MAP
			req.WorkNumber = rsp.WorkNumber
			req.WorkKey = rsp.WorkKey
		} else if rsp.WorkInd == WORK_REDUCE {
			ReduceWork(reducef, rsp.WorkNumber, rsp.Number)
			req.WorkInd = WORK_REDUCE
			req.WorkNumber = rsp.WorkNumber
			req.WorkKey = rsp.WorkKey
		} else if rsp.WorkInd == WORK_WAITING {
			time.Sleep(time.Second * 1)
			req.WorkInd = WORK_WAITING
		} else {
			return
		}
	}

	// uncomment to send the Example RPC to the master.
	// CallExample()

}

func MapWork(mapf func(string, string) []KeyValue, fileName string, mapNumber int, nReduce int) {
	//
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	//
	kva := mapf(fileName, string(content))
	//
	mapResultFiles := make([]*os.File, 0)
	mapResultFileNames := make([]string, 0)
	for i := 0;i < nReduce;i ++ {
		mapResultFileName := fmt.Sprintf("mr-%d-%d", mapNumber, i)
		mapResultFileNames = append(mapResultFileNames, mapResultFileName)
		mapResultFile, err := ioutil.TempFile("", "temp")
		if err != nil {
			log.Fatalf("cannot create temp file %v", mapResultFileName)
		}
		mapResultFiles = append(mapResultFiles, mapResultFile)
	}
	kvas := make([][]KeyValue, 0)
	for i := 0;i < nReduce;i ++ {
		kvas = append(kvas, make([]KeyValue, 0))
	}
	for _, kv := range kva {
		reduceNumber := ihash(kv.Key) % nReduce
		kvas[reduceNumber] = append(kvas[reduceNumber], kv)
	}
	for i, kvs := range kvas {
		enc := json.NewEncoder(mapResultFiles[i])
		for _, kv := range kvs {
			enc.Encode(&kv)
		}
	}
	for i, mapResultFile := range mapResultFiles {
		err := os.Rename(mapResultFile.Name(), mapResultFileNames[i])
		if err != nil {
			log.Fatalf("cannot rename file %v", fileName)
		}
		mapResultFile.Close()
	}
}

func ReduceWork(reducef func(string, []string) string, reduceNumber int, nMap int) {
	kva := []KeyValue{}
	for i := 0;i < nMap;i ++ {
		mapResultFileName := fmt.Sprintf("mr-%d-%d", i, reduceNumber)
		file, err := os.Open(mapResultFileName)
		if err != nil {
			log.Fatalf("cannot open %v", mapResultFileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(kva))

	i := 0
	reduceResultFileName := fmt.Sprintf("mr-out-%d", reduceNumber)
	reduceResultfile, _ := os.Create(reduceResultFileName)
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(reduceResultfile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	reduceResultfile.Close()
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
