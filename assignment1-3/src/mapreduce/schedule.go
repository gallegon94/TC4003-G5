package mapreduce

import (
  "sync"
	"fmt"
)


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
	failed_count_tasks := 0

	for n := 0; n < ntasks; n++ {
		task = append(task, DoTaskArgs{mr.jobName, mr.files[n], phase, n, nios})
	}

	fmt.Println("JALG Starting scheduling")
	fmt.Println(ntasks)
//  wlen := len(mr.workers)
//  cw := 0
	for n := 0; n < ntasks; n++ {
	  found := false;
    debug("JALG leyendo del register Channel\n")
    worker = <- mr.registerChannel
    for found != true {
			for _, w_available := range mr.workers {
        if worker == w_available {
          found = true
          break
        }
      }
      if found == false {
        worker = <- mr.registerChannel
      }
    }

    wg.Add(1)
  	go func(wg *sync.WaitGroup, task *DoTaskArgs, worker string, failed *int) {
//			defer wg.Done()

	  	ok := call(worker, "Worker.DoTask", *task, new(struct{}))
	  	if ok {
				debug("JALG Terminó la tarea con success\n")
				fmt.Println("JALG Terminó la tarea con success")
//	  		task.File = "";
//				debug("JALG escribiendo al register Channel\n")
//				mr.registerChannel <- worker
	  	} else {
				debug("JALG Terminó la tarea con failure\n")
		  	*failed++
	  	}
      wg.Done()
      fmt.Println("JALG saliendo de la tarea")
			mr.registerChannel <- worker
  	}(&wg, &task[n], worker, &failed_count_tasks)
	}


	debug("JALG Saliendo del do task\n")
	fmt.Println("JALG esperando los tasks")

	debug("JALG con %v tareas fallidas \n", failed_count_tasks)
	debug("JALG esperando los tasks\n")
  wg.Wait()
	fmt.Println("JALG tiempo de los tasks terminado")
  debug("JALG terminando los tasks\n")

  if failed_count_tasks > 0 {
	for failed_count_tasks != 0 {
		for n := 0; n < ntasks; n++ {
			if task[n].File != "" {
//				found := false;
//				for found == false {
					worker = <- mr.registerChannel
//					for _, w_available := range mr.workers {
//						if worker == w_available {
//							found = true
//							break
//						}
//					}
//				}

			  go func(task *DoTaskArgs, worker string, failed *int) {
  				ok := call(worker, "Worker.DoTask", *task, new(struct{}))
	  			if ok {
			  		*failed--
						mr.registerChannel <- worker
  				}
	  		}(&task[n], worker, &failed_count_tasks)
			}
		}
	}
//	} else {
//		mr.registerChannel <- worker
	}


	//var wg sync.WaitGroup
	//wg.Add(ntasks)


	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//

	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	//wg.Wait()
	debug("JLASAGU: %v workers, %v address, %v stats\n", mr.workers, mr.address, mr.stats)
	debug("Schedule: %v phase done\n", phase)
}
