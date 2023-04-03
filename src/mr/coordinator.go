package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var mu sync.Mutex

type TaskMetaHolder struct {
	MetaMap map[int]*TaskMetaInfo
}

func (j *TaskMetaHolder) putTask(taskMetaInfo *TaskMetaInfo) bool {
	taskId := taskMetaInfo.TaskPtr.TaskId
	meta, _ := j.MetaMap[taskId]
	if meta != nil {
		// fmt.Println("meta contains task with id = ", taskId)
		return false
	} else {
		j.MetaMap[taskId] = taskMetaInfo
	}
	return true
}

func (j *TaskMetaHolder) getTaskMetaInfo(id int) (bool, *TaskMetaInfo) {
	res, ok := j.MetaMap[id]
	return ok, res
}

func (j *TaskMetaHolder) fireTask(taskId int) bool {
	ok, taskInfo := j.getTaskMetaInfo(taskId)
	if !ok || taskInfo.condition != UndispatchedTask {
		return false
	}
	taskInfo.condition = DispatchedTask
	taskInfo.StartTime = time.Now()
	return true
}

func (j *TaskMetaHolder) checkTaskDone() bool {
	reduceDoneNum := 0
	reduceUndoneNum := 0
	mapDoneNum := 0
	mapUndoneNum := 0
	for _, v := range j.MetaMap {
		if v.TaskPtr.TaskType == MapTask {
			if v.condition == FinishedTask {
				mapDoneNum += 1
			} else {
				mapUndoneNum++
			}
		} else {
			if v.condition == FinishedTask {
				reduceDoneNum++
			} else {
				reduceUndoneNum++
			}
		}
	}

	// fmt.Printf("%d/%d map jobs are done, %d/%d reduce job are done\n",
	// mapDoneNum, mapDoneNum+mapUndoneNum, reduceDoneNum, reduceDoneNum+reduceUndoneNum)

	return (reduceDoneNum > 0 && reduceUndoneNum == 0) || (mapDoneNum > 0 && mapUndoneNum == 0 && reduceUndoneNum == 0)
}

const (
	MapPhase = iota
	ReducePhase
	AllDone
)

type Coordinator struct {
	// Your definitions here.
	JobChannelMap     chan *Task
	TaskChannelReduce chan *Task
	ReducerNum        int
	MapNum            int
	uniqueJobId       int
	CoordinatorState  int
	taskMetaHolder    TaskMetaHolder
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTasks(args *ExampleArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()
	// fmt.Println("coordinator get a request from worker :")

	if c.CoordinatorState == MapPhase {
		if len(c.JobChannelMap) > 0 {
			*reply = *<-c.JobChannelMap
			if !c.taskMetaHolder.fireTask(reply.TaskId) {
				// fmt.Printf("[duplicated task id]task %d is running\n", reply.TaskId)
			}
		} else {
			reply.TaskType = WaittingTask
			if c.taskMetaHolder.checkTaskDone() {
				c.nextPhase()
			}
		}
		return nil
	} else if c.CoordinatorState == ReducePhase {
		if len(c.TaskChannelReduce) > 0 {
			*reply = *<-c.TaskChannelReduce
			if !c.taskMetaHolder.fireTask(reply.TaskId) {
				// fmt.Printf("[duplicated task id]task %d is running\n", reply.TaskId)
			}
		} else {
			reply.TaskType = WaittingTask
			if c.taskMetaHolder.checkTaskDone() {
				c.nextPhase()
			}
		}
		return nil
	} else {
		reply.TaskType = KillTask
	}

	return nil
}

// deal with crash , and task time out
func (c *Coordinator) CrashHandler() {
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		if c.CoordinatorState == AllDone {
			mu.Unlock()
			break
		}

		timeNow := time.Now()
		for _, v := range c.taskMetaHolder.MetaMap {
			// fmt.Println(v)
			if v.condition == DispatchedTask {
				// fmt.Println("task", v.TaskPtr.TaskId, " working for ", timeNow.Sub(v.StartTime))
			}
			if v.condition == DispatchedTask && timeNow.Sub(v.StartTime) > 5*time.Second {
				fmt.Println("detect a crash on task", v.TaskPtr.TaskId)
				switch v.TaskPtr.TaskType {
				case MapTask:
					c.JobChannelMap <- v.TaskPtr
					v.condition = UndispatchedTask
					break
				case ReduceTask:
					c.TaskChannelReduce <- v.TaskPtr
					v.condition = UndispatchedTask
					break
				}

			}
		}
		mu.Unlock()
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {

	// Your code here.
	return c.CoordinatorState == AllDone
}

func (c *Coordinator) generateJobId() int {
	c.uniqueJobId += 1
	return c.uniqueJobId
}

func (c *Coordinator) nextPhase() {
	if c.CoordinatorState == MapPhase {
		c.makeReduceTasks()
		c.CoordinatorState = ReducePhase
	} else if c.CoordinatorState == ReducePhase {
		c.CoordinatorState = AllDone
	}
}

func (c *Coordinator) TaskIsDone(args *Task, reply *ExampleReply) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.TaskType {
	case MapTask:
		ok, meta := c.taskMetaHolder.getTaskMetaInfo(args.TaskId)
		if ok && meta.condition == DispatchedTask {
			meta.condition = FinishedTask
			fmt.Printf("Map Task On %d complete\n", args.TaskId)
		} else {
			// fmt.Println("[duplicated] task done", args.TaskId)
		}
		break
	case ReduceTask:
		// fmt.Printf("Reduce task on %d complete\n", args.TaskId)
		ok, meta := c.taskMetaHolder.getTaskMetaInfo(args.TaskId)
		if ok && meta.condition == DispatchedTask {
			meta.condition = FinishedTask
		} else {
			// fmt.Println("[duplicated] task done", args.TaskId)
		}
	default:
		// fmt.Println("Panic")
	}
	return nil
}

func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateJobId()
		// fmt.Println("making reduce task: ", id)
		taskToDo := Task{
			TaskType:  ReduceTask,
			TaskId:    id,
			InputFile: TmpFileAssignHelper(i, "./"),
		}
		TaskMetaInfo := TaskMetaInfo{
			condition: UndispatchedTask,
			TaskPtr:   &taskToDo,
		}
		c.taskMetaHolder.putTask(&TaskMetaInfo)
		c.TaskChannelReduce <- &taskToDo
	}
	// fmt.Println("done making reduce tasks")

}

func (c *Coordinator) makeMapTask(files []string) {
	for _, v := range files {
		id := c.generateJobId()
		job := Task{
			TaskType:   MapTask,
			InputFile:  []string{v},
			TaskId:     id,
			ReducerNum: c.ReducerNum,
		}

		TaskMetaInfo := TaskMetaInfo{
			condition: UndispatchedTask,
			TaskPtr:   &job,
		}
		c.taskMetaHolder.putTask(&TaskMetaInfo)
		c.JobChannelMap <- &job
	}
	// fmt.Println("done making map tasks")
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		JobChannelMap:     make(chan *Task, len(files)),
		TaskChannelReduce: make(chan *Task, nReduce),
		ReducerNum:        nReduce,
		MapNum:            len(files),
		uniqueJobId:       0,
		CoordinatorState:  MapPhase,
		taskMetaHolder: TaskMetaHolder{
			MetaMap: make(map[int]*TaskMetaInfo, len(files)+nReduce),
		},
	}

	c.makeMapTask(files)
	// Your code here.

	c.server()

	go c.CrashHandler()
	return &c
}

func TmpFileAssignHelper(whichReduce int, tmpFileDirectoryName string) []string {
	var res []string
	path, _ := os.Getwd()
	rd, _ := ioutil.ReadDir(path)
	for _, fi := range rd {
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(whichReduce)) {
			res = append(res, fi.Name())
		}
	}
	return res
}
