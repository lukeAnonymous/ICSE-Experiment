package nodemonitorpackage

import "github.com/ethereum/go-ethereum/common"

// Pair 定义一个用于存储键值对的结构体
type Pair struct {
	Key   common.Address
	Value float64
}

// PairList 定义一个包含Pair类型的切片
type PairList []Pair

func (p PairList) Len() int           { return len(p) }
func (p PairList) Less(i, j int) bool { return p[i].Value > p[j].Value } // 降序排序
func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
