package utils

import (
    "os"
    "testing"
)

func TestExist1(t *testing.T) {
    t.Log(os.TempDir() + "ssds")

    exist := Exist(os.TempDir() + "ssds")
    t.Log(exist)

    if err := os.MkdirAll(os.TempDir()+"abcd", 0644); err != nil {
        t.Error(err)
    }

    exist = Exist(os.TempDir() + "abcd")
    t.Log(exist)
    os.Remove(os.TempDir()+"abcd")
}
