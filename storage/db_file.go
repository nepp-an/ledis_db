package storage

import (
    "errors"
    "fmt"
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
