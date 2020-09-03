package mapreduce

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	m := make(map[string][]string)
	for x := 0; x < nMap; x++ {
		jsonStr, err := ioutil.ReadFile(reduceName(jobName, x, reduceTaskNumber))
		checkError(err)
		values := make([]KeyValue, 0)
		error := json.Unmarshal(jsonStr, &values)
		checkError(error)

		for _, v := range values {
			if _, ok := m[v.Key]; !ok {
				newList := make([]string, 0)
				m[v.Key] = append(newList, v.Value)
			} else {
				m[v.Key] = append(m[v.Key], v.Value)
			}
		}
	}

	f, err := os.Create(mergeName(jobName, reduceTaskNumber))
	log.Print(mergeName(jobName, reduceTaskNumber))
	checkError(err)
	enc := json.NewEncoder(f)
	for key, value := range m {
		enc.Encode(KeyValue{key, reduceF(key, value)})
	}
	f.Close()
}
