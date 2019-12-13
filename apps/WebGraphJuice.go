package main


//input a keyMap map[key][]value, output a map: key -> value    string->int
func WebGraphJuice(keyMap map[string][]string) (map[string][]string,error) {
	var result = make(map[string][]string)

	for key, values := range keyMap{
		result[key] = append(result[key], values...)
	}

	return result, nil
}