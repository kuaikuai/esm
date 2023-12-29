package main

import "fmt"

func SubString(prim string, start int, end int) string {
    if len(prim) == 0 {
        fmt.Println("primitive str is empty")
    }
    if l := len(prim); l < end {
        end = l
    }

    value := prim
    runes := []rune(value)

    safeSubString := string(runes[start:end])

    return safeSubString
}
