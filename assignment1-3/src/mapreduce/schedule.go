package mapreduce

import "sync"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}
	debug("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)
  var task []DoTaskArgs
	var worker string
	var wg sync.WaitGroup
	has_failed_tasks := false
  failed_tasks := make([]bool, ntasks)

	for n := 0; n < ntasks; n++ {
		task = append(task, DoTaskArgs{mr.jobName, mr.files[n], phase, n, nios})
    failed_tasks[n] = false;
	}

  for n := 0; n < ntasks; n++ {
    worker = get_next_worker(mr)
    wg.Add(1)
    go call_worker(mr, &wg, &task[n], worker, &failed_tasks[n], &has_failed_tasks)
  }

  wg.Wait()

  for has_failed_tasks == true {
    has_failed_tasks = false
    for n := 0; n < ntasks; n++ {
      if failed_tasks[n] == true {
        worker = get_next_worker(mr)
        wg.Add(1)
        go call_worker(mr, &wg, &task[n], worker, &failed_tasks[n], &has_failed_tasks)
      }
    }
    wg.Wait()
  }

	debug("Schedule: %v phase done\n", phase)
}

func call_worker(mr *Master, wg *sync.WaitGroup, task *DoTaskArgs, worker string,
                 failed_task *bool, failed *bool) {
  ok := call(worker, "Worker.DoTask", *task, new(struct{}))

  if ok == true {
    *failed_task = false;
    wg.Done()
    mr.registerChannel <- worker
  } else {
    *failed_task = true;
    *failed = true;
    wg.Done()
  }
}

func get_next_worker(mr *Master) string {
  worker_found := false;
  worker := <- mr.registerChannel
  for worker_found != true {
    for _, w_available := range mr.workers {
      if worker == w_available {
        worker_found = true
        break
      }
    }
    if worker_found == false {
      worker = <- mr.registerChannel
    }
  }
  return worker
}
