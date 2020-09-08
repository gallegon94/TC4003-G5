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
	has_failed_tasks := false
  failed_tasks := make([]bool, ntasks+1)

	for n := 0; n < ntasks; n++ {
		task = append(task, DoTaskArgs{mr.jobName, mr.files[n], phase, n, nios})
    failed_tasks[n] = false;
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
  	go func(wg *sync.WaitGroup, task *DoTaskArgs, worker string,
            failed_task *bool, failed *bool) {
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
        *failed_task = true;
		  	*failed = true;
	  	}
      wg.Done()
      fmt.Println("JALG saliendo de la tarea")
			mr.registerChannel <- worker
  	}(&wg, &task[n], worker, &failed_tasks[n], &has_failed_tasks)
	}


	debug("JALG Saliendo del do task\n")
	fmt.Println("JALG esperando los tasks")

//	debug("JALG con %v tareas fallidas \n", failed_count_tasks)
	debug("JALG esperando los tasks\n")
  wg.Wait()
	fmt.Println("JALG tiempo de los tasks terminado")
  debug("JALG terminando los tasks\n")

  for has_failed_tasks == true {
   has_failed_tasks = false
   fmt.Println("JALG hubo tareas fallidas")
   for n := 0; n < ntasks; n++ {
  	if failed_tasks[n] == true {
      found := false;
      debug("JALG leyendo del register Channel\n")
      fmt.Println("JALG leyendo del register Channel")
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
      fmt.Println("JALG Encontramos un worker")
      wg.Add(1)
      go func(wg *sync.WaitGroup, task *DoTaskArgs, worker string,
              failed_task *bool, failed *bool) {
		  	ok := call(worker, "Worker.DoTask", *task, new(struct{}))
		  	if ok {
          *failed_task = false;
          fmt.Println("JALG Terminó la tarea con success")
			  } else {
          *failed = true
        }
        fmt.Println("JALG saliendo de la tarea")
        wg.Done()
        mr.registerChannel <- worker
		  }(&wg, &task[n], worker, &failed_tasks[n], &has_failed_tasks)
	  }
   }
   fmt.Println("JALG Esperando las tasks que fallaron")
   wg.Wait()
   fmt.Println("JALG Terminamos de esperar las tasks")
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
