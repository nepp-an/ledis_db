package ledis_db

import (
    "ledis_db/index"
    "sync"
)

type StrIdx struct {
    mu    sync.RWMutex
    idxList *index.SkipList
}
