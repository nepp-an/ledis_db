package storage

import (
    "hash/crc32"
    "os"
    "testing"
)

func TestNewEntry(t *testing.T) {
    key, value, extra := []byte("key"), []byte("value"), []byte("extra")
    e := NewEntry(key, value, extra, String, 0)
    t.Logf("%+v %+v", e, e.Meta)
}

var testDir = "/Users/anpuqiang/Documents/code/gitlab/src/ledis_db/test.dat"

func TestEntry_Encode(t *testing.T) {
    t.Run("key_value", func(t *testing.T) {
        e := &Entry{
            Meta: &Meta{
                Key:   []byte("test_key_0001"),
                Value: []byte("test_value_0001"),
            },
        }

        e.Meta.KeySize = uint32(len(e.Meta.Key))
        e.Meta.ValueSize = uint32(len(e.Meta.Value))

        encVal, err := e.Encode()
        if err != nil {
            t.Fatal(err)
        }
        t.Log(e.Size())
        t.Log(encVal)
        os.Remove(testDir)
        if encVal != nil {
            file, _ := os.OpenFile(testDir, os.O_CREATE|os.O_WRONLY, 0644)
            file.Write(encVal)
        }
    })

    t.Run("no_value", func(t *testing.T) {
        e := &Entry{
            Meta: &Meta{
                Key: []byte("test_key_0001"),
            },
        }

        e.Meta.KeySize = uint32(len(e.Meta.Key))
        e.Meta.ValueSize = uint32(len(e.Meta.Value))

        encVal, err := e.Encode()
        if err != nil {
            t.Fatal(err)
        }
        t.Log(e.Size())
        t.Log(encVal)
    })

    //key为空的情况
    t.Run("no_key", func(t *testing.T) {
        e := &Entry{
            Meta: &Meta{
                Key:   []byte(""),
                Value: []byte("val_001"),
            },
        }

        e.Meta.KeySize = uint32(len(e.Meta.Key))
        e.Meta.ValueSize = uint32(len(e.Meta.Value))

        if encode, err := e.Encode(); err != nil {
            if err != ErrInvalidEntry {
                t.Error(err)
            }
        } else {
            t.Log(encode)
        }
    })
}

func TestDecode(t *testing.T) {
    if file, err := os.OpenFile(testDir, os.O_RDONLY, os.ModePerm); err != nil {
        t.Error("Open File err ", err)
    } else {
        buf := make([]byte, entryHeaderSize)
        var offset int64 = 0
        if _, err := file.ReadAt(buf, offset); err != nil {
            t.Error("read data error ", err)
        } else {
            t.Log(buf)
            e, decodeErr := DecodeEntryHeader(buf)
            if decodeErr != nil {
                t.Error("decode data error ", decodeErr)
            } else {
                // read key
                offset += entryHeaderSize
                if e.Meta.KeySize > 0 {
                    key := make([]byte, e.Meta.KeySize)
                    file.ReadAt(key, offset)
                    t.Logf("Key = %s", string(key))
                }

                //read value
                offset += int64(e.Meta.KeySize)
                if e.Meta.ValueSize > 0 {
                    val := make([]byte, e.Meta.ValueSize)
                    file.ReadAt(val, offset)
                    e.Meta.Value = val
                    t.Logf("Value = %s", string(val))
                }

                crc := crc32.ChecksumIEEE(e.Meta.Value)
                t.Log(crc, e.crc32)
            }
        }
        os.Remove(testDir)
    }

}