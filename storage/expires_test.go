package storage

import (
    "fmt"
    "testing"
)

const (
    expiresPath = "/Users/anpuqiang/Documents/code/gitlab/src/ledis_db/db.expires"
)
func TestExpires_SaveExpires(t *testing.T) {
    expires := make(Expires)
    expires["key_001"] = 123456
    expires["key_002"] = 1234567
    expires["key_003"] = 23456
    expires["key_005"] = 34567

    err := expires.SaveExpires(expiresPath)
    if err != nil {
        t.Error(err)
    }
}

func TestLoadExpires(t *testing.T) {
    newExpires := LoadExpires(expiresPath)
    t.Logf("%+v\n", newExpires)
    for k, v := range newExpires {
        fmt.Println(k, ":", v)
    }
}