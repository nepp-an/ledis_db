package ledis_db

type DataIndexMode int

const (
    // KeyValueRamMode key value均存储在内存中
    KeyValueRamMode  DataIndexMode = iota
    // KeyOnlyRamMode 只有key存储在内存中
    KeyOnlyRamMode
)

const (
    // DefaultAddr 默认服务器地址
    DefaultAddr = "127.0.0.1:5200"

    DefaultDirPath = "/tmp/ledisdb_server"

    // DefaultBlockSize 默认数据块文件大小：16MB
    DefaultBlockSize = 16 * 1024 * 1024

    // DefaultMaxKeySize 默认的key最大值 128字节
    DefaultMaxKeySize = uint32(128)

    // DefaultMaxValueSize 默认的value最大值 1MB
    DefaultMaxValueSize = uint32(1 * 1024 * 1024)

    // DefaultReclaimThreshold 默认回收磁盘空间的阈值，当已封存文件个数到达 4 时，可进行回收
    DefaultReclaimThreshold = 4
)
