package index

import (
    "bytes"
    "fmt"
    "math/rand"
    "time"
)

// SkipList
// 可以在O(log(n))的时间复杂度下进行插入、删除和查找


const UP_LEVELS_ABILITY = 500
const UP_LEVELS_TOTAL = 1000

type skipListNode struct {
    key   []byte
    val   interface{}
    next  *skipListNode
    pre   *skipListNode
    up    *skipListNode
    down  *skipListNode
}

type skipList struct {
    head   *skipListNode  //左上节点
    tail   *skipListNode  //右上节点
    size   int
    levels int
}

func NewSkipList_() *skipList {
    sl := new(skipList)
    sl.head = new(skipListNode)
    sl.tail = new(skipListNode)
    sl.head.key = []byte("")
    sl.tail.key = []byte("#####")

    sl.head.next = sl.tail
    sl.tail.pre = sl.head

    sl.size = 0
    sl.levels = 1

    return sl
}

func (sl *skipList) Size() int {
    return sl.size
}

func (sl *skipList) Levels() int {
    return sl.levels
}

func (sl *skipList) Get(key []byte) interface{} {
    node := sl.findNode(key)
    if bytes.Compare(node.key, key) == 0 {
        return node.val
    } else {
        return nil
    }
}

func (sl *skipList) Insert(key []byte, val interface{}) {
    f := sl.findNode(key)
    if bytes.Compare(f.key, key) == 0 {
        f.val = val
        return
    }
    curNode := new(skipListNode)
    curNode.key = key
    curNode.val = val

    sl.insertAfter(f, curNode)

    rander := rand.New(rand.NewSource(time.Now().UnixNano()))

    curlevels := 1
    for rander.Intn(UP_LEVELS_TOTAL) < UP_LEVELS_ABILITY {
        curlevels++
        if curlevels > sl.levels {
            sl.newlevels()
        }

        for f.up == nil {
            f = f.pre
        }
        f = f.up
        tmpNode := &skipListNode{key: key}

        curNode.up = tmpNode
        tmpNode.down = curNode
        sl.insertAfter(f, tmpNode)

        curNode = tmpNode
    }

    sl.size++
}

func (sl *skipList) Remove(key []byte) interface{} {
    f := sl.findNode(key)
    if bytes.Compare(f.key, key) != 0 {
        return nil
    }
    v := f.val

    for f != nil {
        f.pre.next = f.next
        f.next.pre = f.pre
        f = f.up
    }
    return v
}

func (sl *skipList) newlevels() {
    nhead := &skipListNode{key: []byte("")}
    ntail := &skipListNode{key: []byte("#####")}
    nhead.next = ntail
    ntail.pre = nhead

    sl.head.up = nhead
    nhead.down = sl.head
    sl.tail.up = ntail
    ntail.down = sl.tail

    sl.head = nhead
    sl.tail = ntail
    sl.levels++
}

func (sl *skipList) insertAfter(pNode *skipListNode, curNode *skipListNode) {
    curNode.next = pNode.next
    curNode.pre = pNode
    pNode.next.pre = curNode
    pNode.next = curNode
}

func (sl *skipList) findNode(key []byte) *skipListNode {
    p := sl.head

    for p != nil {
        if bytes.Compare(p.key, key) == 0 {
            if p.down == nil {
                return p
            }
            p = p.down
        } else if bytes.Compare(p.key, key) < 0 {
            if bytes.Compare(p.next.key, key) > 0 {
                if p.down == nil {
                    return p
                }
                p = p.down
            } else {
                p = p.next
            }
        }
    }
    return p
}

func (sl *skipList) Print() {

    mapScore := make(map[string]int)

    p := sl.head
    for p.down != nil {
        p = p.down
    }
    index := 0
    for p != nil {
        mapScore[string(p.key)] = index
        p = p.next
        index++
    }
    p = sl.head
    for i := 0; i < sl.levels; i++ {
        q := p
        preIndex := 0
        for q != nil {
            s := q.key
            if bytes.Compare(s, []byte("")) == 0 {
                fmt.Printf("%s", "BEGIN")
                q = q.next
                continue
            }
            index := mapScore[string(s)]
            c := (index - preIndex - 1) * 6
            for m := 0; m < c; m++ {
                fmt.Print("-")
            }
            if bytes.Compare(s, []byte("#####")) == 0 {
                fmt.Printf("-->%s\n", "END")
            } else {
                fmt.Printf("-->%3d", s)
                preIndex = index
            }
            q = q.next
        }
        p = p.down
    }
}

