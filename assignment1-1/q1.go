package cos418_hw1_1

import (
	"fmt"
	"io/ioutil"
	"regexp"
	"sort"
	"strings"
)

// Find the top K most common words in a text document.
// 	path: location of the document
//	numWords: number of words to return (i.e. k)
//	charThreshold: character threshold for whether a token qualifies as a word,
//		e.g. charThreshold = 5 means "apple" is a word but "pear" is not.
// Matching is case insensitive, e.g. "Orange" and "orange" is considered the same word.
// A word comprises alphanumeric characters only. All punctuations and other characters
// are removed, e.g. "don't" becomes "dont".
// You should use `checkError` to handle potential errors.
func topWords(path string, numWords int, charThreshold int) []WordCount {
	var m = make(map[string]*WordCount)

	var test = formatedStr(path)
	var slice = strings.Split(string(test), " ")
	for _, value := range slice {
		value = strings.ToLower(value)
		if len(value) >= charThreshold {
			if _, ok := m[value]; !ok {
				m[value] = &WordCount{Word: value, Count: 1}
			} else {
				m[value].Count++
			}
		}
	}

	values := []WordCount{}
	for _, value := range m {
		values = append(values, *value)
	}

	sortWordCounts(values)

	return values[0:numWords]
}

func formatedStr(path string) []byte {
	content, err := ioutil.ReadFile(path)
	checkError(err)
	re := regexp.MustCompile(`\s+`)
	content = re.ReplaceAll(content, []byte(" "))

	re2 := regexp.MustCompile(`[^0-9a-zA-Z ]`)
	return re2.ReplaceAll(content, []byte(""))
}

// A struct that represents how many times a word is observed in a document
type WordCount struct {
	Word  string
	Count int
}

func (wc WordCount) String() string {
	return fmt.Sprintf("%v: %v", wc.Word, wc.Count)
}

// Helper function to sort a list of word counts in place.
// This sorts by the count in decreasing order, breaking ties using the word.
// DO NOT MODIFY THIS FUNCTION!
func sortWordCounts(wordCounts []WordCount) {
	sort.Slice(wordCounts, func(i, j int) bool {
		wc1 := wordCounts[i]
		wc2 := wordCounts[j]
		if wc1.Count == wc2.Count {
			return wc1.Word < wc2.Word
		}
		return wc1.Count > wc2.Count
	})
}
