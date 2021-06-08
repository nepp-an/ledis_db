package ledis_db

import (
    "ledis_db/storage"
    "sync"
)

type (
    ArchivedFiles   map[uint32]*storage.DBFile
    LedisDB struct {
        activeFile  *storage.DBFile
        activeFileID uint32
        archFiles    ArchivedFiles
        strIndex     *StrIdx
        mu           sync.RWMutex
        meta         *storage.DBMeta
        expires     storage.Expires

    }
)