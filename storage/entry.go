package storage

import (
    "encoding/binary"
    "errors"
    "hash/crc32"
)

var (
    ErrInvalidEntry = errors.New("storage/entry: invalid entry")
    ErrInvalidCrc   = errors.New("storage/entry: invalid crc")
)

const (
    //KeySize, ValueSize, ExtraSize, crc32 均为 uint32 类型，各占 4 字节
    //Type 和 Mark 占 2 + 2
    //4 + 4 + 4 + 4 + 2 + 2 = 20
    entryHeaderSize = 20
)

type (
	Entry struct {
		Meta  *Meta
		Type  uint16 //数据类型
		Mark  uint16 //操作类型
		crc32 uint32 //校验和
	}

	Meta struct {
		Key       []byte
		Value     []byte
		Extra     []byte
		KeySize   uint32
		ValueSize uint32
		ExtraSize uint32
	}
)

// Entry中Type的数据结构类型
const (
    String uint16 = iota
    List
    Hash
    Set
    ZSet
)

func NewEntry(key, value, extra []byte, typ, mark uint16) *Entry {
    return &Entry{
        Meta:  &Meta{
            Key:       key,
            Value:     value,
            Extra:     extra,
            KeySize:   uint32(len(key)),
            ValueSize: uint32(len(value)),
            ExtraSize: uint32(len(extra)),
        },
        Type:  typ,
        Mark:  mark,
        crc32: 0,
    }
}

func (e *Entry) Size() uint32 {
    return entryHeaderSize + e.Meta.ExtraSize + e.Meta.KeySize + e.Meta.ValueSize
}

func (e *Entry) Encode() ([]byte, error) {
    if e == nil || e.Meta.KeySize == 0 {
        return nil, ErrInvalidEntry
    }

    buf := make([]byte, e.Size())
    binary.BigEndian.PutUint32(buf[4:8], e.Meta.KeySize)
    binary.BigEndian.PutUint32(buf[8:12], e.Meta.ValueSize)
    binary.BigEndian.PutUint32(buf[12:16], e.Meta.ExtraSize)
    binary.BigEndian.PutUint16(buf[16:18], e.Type)
    binary.BigEndian.PutUint16(buf[18:20], e.Mark)
    copy(buf[20:20+e.Meta.KeySize], e.Meta.Key)
    copy(buf[20+e.Meta.KeySize:20+e.Meta.KeySize+e.Meta.ValueSize], e.Meta.Value)
    if e.Meta.ExtraSize > 0 {
        copy(buf[20+e.Meta.KeySize+e.Meta.ValueSize:20+e.Meta.KeySize+e.Meta.ValueSize+e.Meta.ExtraSize], e.Meta.Extra)
    }

    crc := crc32.ChecksumIEEE(e.Meta.Value)
    binary.BigEndian.PutUint32(buf[0:4], crc)
    return buf, nil
}


// Decode 解码字节数组，返回Entry
func Decode(buf []byte) (*Entry, error) {
    ks := binary.BigEndian.Uint32(buf[4:8])
    vs := binary.BigEndian.Uint32(buf[8:12])
    es := binary.BigEndian.Uint32(buf[12:16])
    t := binary.BigEndian.Uint16(buf[16:18])
    mark := binary.BigEndian.Uint16(buf[18:20])
    crc := binary.BigEndian.Uint32(buf[0:4])

    return &Entry{
        Meta: &Meta{
            KeySize:   ks,
            ValueSize: vs,
            ExtraSize: es,
        },
        Type:  t,
        Mark:  mark,
        crc32: crc,
    }, nil
}