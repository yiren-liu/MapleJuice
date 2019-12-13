package main

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)


//input a file Scanner, output a slice of map[10]: key -> value
func WordcountMaple(scanner *bufio.Scanner) ([]map[string]string,error) {
	//parse input by line
	var counter = make(map[string]int)
	var mapleResult []map[string]string
	count := 0

	for scanner.Scan() {
		wordList := strings.Fields(scanner.Text())

		for _, word := range wordList {
			counter[word]++
		}

		count++
		if count == 10 {
			map10 := make(map[string]string)
			for word, count := range counter {
				map10[word] = strconv.Itoa(count)
			}
			mapleResult = append(mapleResult, map10)
			count = 0
			counter = make(map[string]int)
		}
	}
		if count != 0{
			map10 := make(map[string]string)
			for word, count := range counter {
				map10[word] = strconv.Itoa(count)
			}
			mapleResult = append(mapleResult, map10)
		}

		if err := scanner.Err(); err != nil{
			fmt.Println("scanner error: ", err)
			return []map[string]string{}, err
		}

		return mapleResult, nil
}

