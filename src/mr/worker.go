package mr

import (
	"encoding/json"
	"fmt"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io/ioutil"
import "sort"
import "time"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	log.SetFlags(log.Ltime | log.Lshortfile)
//	log.Printf("worker starts")

	for {
		isSuccess, reply := AskForTask()
		if isSuccess == false || reply.Done == true {
			break
		}

		if reply.Valid == true {
			var err error
			if reply.Task.Type == Map {
				err = ExecMapTask(mapf, reply.Task.Idx, reply.Task.Inputfile, reply.Reducenr)
			} else {
				err = ExecReduceTask(reducef, reply.Task.Idx, reply.Mapnr, reply.Reducenr)
			}

			task := reply.Task
			task.Err = err
			RetResult(task)
		}

		time.Sleep(time.Duration(100) * time.Millisecond) //100ms
	}

//	log.Printf("worker exit")
}

func RetResult(task Task) {
	args  := Args{}
	reply := Reply{} //dont need it

	args.Task = task
	call("Master.GetResult", &args, &reply)
}

func ExecMapTask(mapf func(string, string) []KeyValue, idx int, filename string, nReduce int) error {
//	log.Printf("execute task map %d %s", idx, filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %d: %v", idx, filename)
		return err
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
		return err
	}

	file.Close()

	kva := mapf(filename, string(content))
	sort.Sort(ByKey(kva))

	for i := 0; i < nReduce; i++ {
		oname := fmt.Sprintf("mr-%d-%d", idx, i)
		ofile, _ := ioutil.TempFile(".", oname)

		for _, kv := range kva {
			if ihash(kv.Key) % nReduce == i {
				enc := json.NewEncoder(ofile)
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("write %v %v into %s failed!\n", kv.Key, kv.Value, ofile.Name)
				}
			}
		}

		ofile.Close()
		os.Rename(ofile.Name(), oname)
	}

	return nil
}

func ExecReduceTask(reducef func(string, []string) string, idx int, nMap int, nReduce int) error {
//	log.Printf("execute task reduce %d", idx)
	oname := fmt.Sprintf("mr-out-%d", idx)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	//get all kv pairs with the same ihash(kv.Key) % nReduce
	intermediate := []KeyValue{}
	for i:= 0; i < nMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, idx)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %d: %v", idx, filename)
			continue
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}

			if ihash(kv.Key) % nReduce == idx {
				intermediate = append(intermediate, kv)
			}
		}

		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	// call Reduce on each distinct key in intermediate[], and print the result to mr-out-idx.
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	return nil
}

func AskForTask() (bool, Reply) {
	args  := Args{} //dont need it
	reply := Reply{}

	isSuccess := call("Master.AssignTask", &args, &reply)

	return isSuccess, reply
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
//		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
