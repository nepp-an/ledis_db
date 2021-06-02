package storage

import (
    "errors"
    "fmt"
    "hash/crc32"
    "os"
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
