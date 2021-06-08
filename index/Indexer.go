package index

import "ledis_db/storage"

type Indexer struct {
    Meta    *storage.Meta  //元数据信息
    FileId  uint32         //文件id
    EntrySize uint32       //Entry大小
    Offset  int64          //Entry数据查询起始位置
}
