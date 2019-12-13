package main

import (
"bufio"
"fmt"
"strings"
)


//input a file Scanner, output a slice of map[10]: key -> value
func WebGraphMaple(scanner *bufio.Scanner) ([]map[string]string,error) {
	//parse input by line
	var mapleResult []map[string]string
	count := 0
	var map10 = make(map[string]string)

	for scanner.Scan() {
		Src2Dst := strings.Fields(scanner.Text())

		map10[Src2Dst[1]] = Src2Dst[0]

		count++
		if count == 10 {
			mapleResult = append(mapleResult, map10)
			count = 0
			map10 = make(map[string]string)
		}
	}
	if count != 0{
		mapleResult = append(mapleResult, map10)
	}

	if err := scanner.Err(); err != nil{
		fmt.Println("scanner error: ", err)
		return []map[string]string{}, err
	}

	return mapleResult, nil
}

