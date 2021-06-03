package storage

import "testing"

const (
    path = "/Users/anpuqiang/Documents/code/gitlab/src/ledis_db"
    fileID1 = 0
    fileID2 = 1
)

func TestNewDBFile(t *testing.T) {
    newOne := func(method FileRWMethod) {
        file, err := NewDBFile(path, fileID1, method, 0)
        if err != nil {
            t.Error("new db file error ", err)
        }
        t.Logf("%+v \n", file)
        t.Log(file.File == nil)
    }

    t.Run("file io", func(t *testing.T) {
        newOne(FileIO)
    })
}

func TestDBFile_Write(t *testing.T) {
    df, err := NewDBFile(path, fileID1, FileIO, 0)
    if err != nil {
        t.Error(err)
    }
    entry1 := &Entry{
        Meta:  &Meta{
            Key:       []byte("Key001"),
            Value:     []byte("Value001"),
        },
    }
    entry1.Meta.KeySize = uint32(len(entry1.Meta.Key))
    entry1.Meta.ValueSize = uint32(len(entry1.Meta.Value))

    entry2 := &Entry{
        Meta:  &Meta{
            Key:       []byte("Key002"),
            Value:     []byte("Value002"),
        },
    }
    entry2.Meta.KeySize = uint32(len(entry2.Meta.Key))
    entry2.Meta.ValueSize = uint32(len(entry2.Meta.Value))

    err = df.Write(entry1)
    t.Logf("offset is %d",df.Offset)
    err = df.Write(entry2)
    t.Logf("offset is %d",df.Offset)

    if err != nil {
        t.Error("df.Write failed: ", err)
    }

    defer func() {
        df.Close(true)
    }()
}

func TestDBFile_Read(t *testing.T) {
    df, _ := NewDBFile(path, fileID1, FileIO, 0)
    readEntry := func(offset int64) *Entry {
        if e, err := df.Read(offset); err != nil {
            t.Error("read db File error ", err)
        } else {
            return e
        }
        return nil
    }

    e1 := readEntry(0)
    t.Log(e1)
    t.Log(string(e1.Meta.Key), e1.Meta.KeySize, string(e1.Meta.Value), e1.Meta.ValueSize, e1.crc32)
    e2 := readEntry(34)
    t.Log(e2)
    t.Log(string(e2.Meta.Key), e2.Meta.KeySize, string(e2.Meta.Value), e2.Meta.ValueSize, e2.crc32)
    defer df.Close(false)

}