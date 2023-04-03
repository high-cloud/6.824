package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

type ByKey []KeyValue

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
	alive := true

	for alive {

		// uncomment to send the Example RPC to the coordinator.
		task := CallGetTask()

		switch task.TaskType {
		case MapTask:
			{
				// fmt.Println("do map", task.TaskId)
				DoMap(mapf, task)
				TaskIsDone(task)
			}
			break
		case ReduceTask:
			{
				// fmt.Println("do reduce ", task.TaskId)
				DoReduce(reducef, task)
				TaskIsDone(task)
			}
			break
		case WaittingTask:
			// fmt.Println("get waitting")
			time.Sleep(time.Second)
			break
		case KillTask:
			// fmt.Println("get killer")
			alive = false
		}
	}

}

func TaskIsDone(task *Task) {
	args := task
	reply := &ExampleReply{}
	// fmt.Println("task is done, id: ", task.TaskId)
	call("Coordinator.TaskIsDone", &args, &reply)
}

func DoReduce(reducef func(string, []string) string, task *Task) {
	reduceFileNum := task.TaskId
	intermediate := readFromLocalFile(task.InputFile)
	sort.Sort(ByKey(intermediate))
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file")
	}
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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", reduceFileNum)
	os.Rename(tempFile.Name(), oname)
}

func readFromLocalFile(files []string) []KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, _ := os.Open(filepath)
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
	return kva
}

func DoMap(mapf func(string, string) []KeyValue, task *Task) {
	var intermediate []KeyValue

	filename := task.InputFile[0]

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate = mapf(filename, string(content))

	// initialize and loop over intermediate
	rn := task.ReducerNum
	HashedKV := make([][]KeyValue, rn)

	for _, kv := range intermediate {
		HashedKV[ihash(kv.Key)%rn] = append(HashedKV[ihash(kv.Key)%rn], kv)
	}

	for i := 0; i < rn; i++ {
		outputFileName := "mr-tmp-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		outputFile, _ := os.Create(outputFileName)
		enc := json.NewEncoder(outputFile)
		for _, kv := range HashedKV[i] {
			enc.Encode(kv)
		}
		outputFile.Close()
	}
}

//
// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		// fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		// fmt.Printf("call failed!\n")
	}
}

// get work from coordinator
func CallGetTask() *Task {
	args := ExampleArgs{}

	args.X = 0

	reply := Task{}

	ok := call("Coordinator.AssignTasks", &args, &reply)

	if ok {
		// fmt.Println("get task: ", &reply)
	} else {
		// fmt.Println("call failed!")
	}

	return &reply

}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}
