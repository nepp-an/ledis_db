package utils

import "os"

func Exist(path string) bool {
    if _, err := os.Stat(path); os.IsNotExist(err) {
        return false
    }

    return true
}
