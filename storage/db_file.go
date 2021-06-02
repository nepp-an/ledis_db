package storage

import (
    "errors"
    "fmt"
    "hash/crc32"
    "io/ioutil"
    "os"
    "sort"
    "strconv"
    "strings"
)

const (
    FilePerm = 0644   //默认创建文件权限 创建人可读可写，其他人只可读

    // DBFileFormatName 默认数据文件名称格式化
    DBFileFormatName = "%09d.data"

    PathSeparator = string(os.PathSeparator)
)

var (
    ErrEmptyEntry = errors.New("storage/db_file: entry is empty")
)

// FileRWMethod 文件数据读写方式
type FileRWMethod uint8

const (
    // 文件数据读写使用系统IO
    FileIO FileRWMethod = iota
)

type DBFile struct {
    Id uint32
    path string
    File *os.File
    Offset int64
    method FileRWMethod
}

func NewDBFile(path string, fileId uint32, method FileRWMethod, blockSize int64) (*DBFile, error) {
    filePath := path + PathSeparator + fmt.Sprintf(DBFileFormatName, fileId)

    file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, FilePerm)
    if err != nil {
        return nil, err
    }

    df := &DBFile{Id: fileId, path: path, Offset: 0, method: method}

    if method == FileIO {
        df.File = file
    }

    return df, nil
}

func (df *DBFile) readBuf(offset int64, n int64) ([]byte, error) {
    buf := make([]byte, n)
    if df.method == FileIO {
        _, err := df.File.ReadAt(buf, offset)
        if err != nil {
            return nil, err
        }
    }

    return buf, nil
}

func (df *DBFile) Read(offset int64) (e *Entry, err error) {
    var buf []byte
    if buf, err = df.readBuf(offset, entryHeaderSize); err != nil {
        return
    }
    if e, err = DecodeEntryHeader(buf); err != nil {
        return
    }

    offset += entryHeaderSize
    if e.Meta.KeySize > 0 {
        var key []byte
        if key, err = df.readBuf(offset, int64(e.Meta.KeySize)); err != nil {
            return
        }
        e.Meta.Key = key
    }

    offset += int64(e.Meta.KeySize)
    if e.Meta.ValueSize > 0 {
        var val []byte
        if val, err = df.readBuf(offset, int64(e.Meta.ValueSize)); err != nil {
            return
        }
        e.Meta.Value = val
    }

    offset += int64(e.Meta.ValueSize)
    if e.Meta.ExtraSize > 0 {
        var extra []byte
        if extra, err = df.readBuf(offset, int64(e.Meta.ExtraSize)); err != nil {
            return
        }
        e.Meta.Extra = extra
    }
    checkCrc := crc32.ChecksumIEEE(e.Meta.Value)
    if checkCrc != e.crc32 {
        return nil, ErrInvalidEntry
    }

    return e, nil
}

func (df *DBFile) Write(e *Entry) error {
    if e == nil || e.Meta.KeySize == 0 {
        return ErrEmptyEntry
    }

    method := df.method
    writeOffSet := df.Offset
    if encodeVal, err := e.Encode(); err != nil {
        return err
    } else {
        if method == FileIO {
            if _, err := df.File.WriteAt(encodeVal, writeOffSet); err != nil {
                return err
            }
        }
    }

    df.Offset += int64(e.Size())
    return nil
}

func (df *DBFile) Close(sync bool) (err error) {
    if sync {
        err = df.Sync()
    }
    if df.File != nil {
        err = df.File.Close()
    }
    return
}

// sync in-memory content into disk
func (df *DBFile) Sync() error {
    if df.File != nil {
        return df.File.Sync()
    }
    return nil
}

func Build(path string, method FileRWMethod, blockSize int64) (map[uint32]*DBFile, uint32, error) {
    dir, err := ioutil.ReadDir(path)
    if err != nil {
        return nil, 0, err
    }

    var fileIds []int
    for _, file := range dir {
        if strings.HasPrefix(file.Name(), "data") {
            splitNames := strings.Split(file.Name(), ".")
            id, _ := strconv.Atoi(splitNames[0])
            fileIds = append(fileIds, id)
        }
    }

    sort.Ints(fileIds)
    var activeFileId uint32 = 0    //最后一个为active
    archFiles := make(map[uint32]*DBFile)
    if len(fileIds) > 0 {
        activeFileId = uint32(fileIds[len(fileIds)-1])
    }

    for i:=0;i<len(fileIds)-1;i++ {
        id := fileIds[i]
        file, err := NewDBFile(path, uint32(id), method, blockSize)
        if err != nil {
            return nil, activeFileId, err
        }
        archFiles[uint32(id)] = file
    }

    return archFiles, activeFileId, nil

}
