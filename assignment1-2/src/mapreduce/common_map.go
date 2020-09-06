package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"os"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	content, err := ioutil.ReadFile(inFile)
	checkError(err)
	results := mapF(inFile, string(content))
	full_slice := make([][]KeyValue, nReduce)
	for _, value := range results {
		index := ihash(value.Key) % uint32(nReduce)
		full_slice[index] = append(full_slice[index], value)
	}
	for index, value := range full_slice {
		f, err := os.Create(reduceName(jobName, mapTaskNumber, index))
		checkError(err)
		j, err := json.Marshal(value)
		checkError(err)
		f.Write(j)
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
