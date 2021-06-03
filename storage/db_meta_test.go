package storage

import "testing"

func TestDBMeta_Store(t *testing.T) {
    m := &DBMeta{43}
    if err := m.Store("/Users/anpuqiang/Documents/code/gitlab/src/ledis_db/db.Meta"); err != nil {
        t.Error(err)
    }
}

func TestLoad(t *testing.T) {
    m := LoadMeta("/Users/anpuqiang/Documents/code/gitlab/src/ledis_db/db.Meta")
    t.Logf("%+v \n", m)
    t.Log(m == nil)
}