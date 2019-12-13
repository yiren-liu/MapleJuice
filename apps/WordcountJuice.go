package main

import (
	"strconv"
)


//input a keyMap map[key][]value, output a map: key -> value    string->int
func WordcountJuice(keyMap map[string][]string) (map[string][]string,error) {
	var counter = make(map[string]int)
	var result = make(map[string][]string)

	for key, values := range keyMap {
		for _, value := range values {
			v,_ := strconv.Atoi(value)
			counter[key] += v
		}
	}

	for key, value := range counter{
		result[key] = []string{strconv.Itoa(value)}
	}

		return result, nil
}