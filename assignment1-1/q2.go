package cos418_hw1_1

import (
	"bufio"
	"io"
	"os"
	"strconv"
)

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
	count := 0
	for i := range nums {
		count += i
	}
	out <- count
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {
	worker_ip := make(chan int, 3*num)
	worker_op := make(chan int, num)
	r, error := os.Open(fileName)
	checkError(error)
	list, err := readInts(r)
	checkError(err)
	for i := 0; i < num; i++ {
		go sumWorker(worker_ip, worker_op)
	}
	for _, number := range list {
		worker_ip <- number
	}

	close(worker_ip)

	count := 0
	for j := 0; j < num; j++ {
		count += <-worker_op
	}

	return count
}

func insert(path string, put chan int) {
	r, _ := os.Open(path)
	list, _ := readInts(r)
	for _, v := range list {
		put <- v
	}
	close(put)
}

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var elems []int
	for scanner.Scan() {
		val, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return elems, err
		}
		elems = append(elems, val)
	}
	return elems, nil
}
