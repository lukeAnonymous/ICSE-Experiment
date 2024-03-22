package scheduler

import (
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
)

const maxUniqueKeys = 65563

type TxnScheduler struct {
	//每笔交易读取的合约
	txReadSet [][]uint64
	//每笔交易写入的合约
	txWriteSet [][]uint64
	//调用每笔智能合约的交易
	keyReadTxMap  map[common.Address][]uint32
	keyWriteTxMap map[common.Address][]uint32
	blocks        types.Blocks
	//交易数量
	uniqueKeyCounter uint32
	//对应每个交易的对应表关系
	uniqueKeyMap map[common.Address]uint32
	//总共需要调度的交易数量
	pendingTxns []common.Hash

	unionSet *UnionSet
}

type TxIndex struct {
	Index int
	TxID  common.Hash
}

func NewTxnScheduler(size int) *TxnScheduler {
	return &TxnScheduler{
		txReadSet:        make([][]uint64, size),
		txWriteSet:       make([][]uint64, size),
		keyReadTxMap:     make(map[common.Address][]uint32),
		keyWriteTxMap:    make(map[common.Address][]uint32),
		uniqueKeyMap:     make(map[common.Address]uint32),
		uniqueKeyCounter: 0,
		pendingTxns:      make([]common.Hash, 0),
		unionSet:         NewUnionSet(),
	}
}

func (scheduler *TxnScheduler) ScheduleTxn(txHash common.Hash, readKeys []common.Address, writeKeys []common.Address) bool {
	tid := uint32(len(scheduler.pendingTxns))

	readSet := make([]uint64, maxUniqueKeys/64)
	writeSet := make([]uint64, maxUniqueKeys/64)
	scheduler.unionSet.AddUnionSet(tid)

	for _, readKey := range readKeys {
		key, ok := scheduler.uniqueKeyMap[readKey]
		if !ok {
			scheduler.uniqueKeyMap[readKey] = scheduler.uniqueKeyCounter
			key = scheduler.uniqueKeyCounter
			scheduler.uniqueKeyCounter += 1
		}

		if key >= maxUniqueKeys {
			return false
		}

		scheduler.keyReadTxMap[readKey] = append(scheduler.keyReadTxMap[readKey], tid)
		index := key / 64
		readSet[index] |= (uint64(1) << (key % 64))
	}

	for _, writeKey := range writeKeys {
		key, ok := scheduler.uniqueKeyMap[writeKey]
		if !ok {
			scheduler.uniqueKeyMap[writeKey] = scheduler.uniqueKeyCounter
			key = scheduler.uniqueKeyCounter
			scheduler.uniqueKeyCounter += 1
		}

		if key >= maxUniqueKeys {
			return false
		}

		index := key / 64
		writeSet[index] |= (uint64(1) << (key % 64))
		scheduler.keyReadTxMap[writeKey] = append(scheduler.keyWriteTxMap[writeKey], tid)
	}

	scheduler.txReadSet[tid] = readSet
	scheduler.txWriteSet[tid] = writeSet

	for i := uint32(0); i < tid; i++ {
		for k := uint32(0); k < (maxUniqueKeys / 64); k++ {
			if scheduler.unionSet.Find(i) == scheduler.unionSet.Find(tid) {
				break
			}
			if (scheduler.txReadSet[i][k]&scheduler.txWriteSet[tid][k] != 0) ||
				(scheduler.txWriteSet[i][k]&scheduler.txWriteSet[tid][k] != 0) ||
				(scheduler.txWriteSet[i][k]&scheduler.txReadSet[tid][k] != 0) {
				scheduler.unionSet.Merge(i, tid)
			}
		}
	}

	scheduler.pendingTxns = append(scheduler.pendingTxns, txHash)

	return true
}

func (scheduler *TxnScheduler) Process() [][]TxIndex {
	txCount := uint32(len(scheduler.pendingTxns))

	unionSet := scheduler.unionSet

	subGraphs := make([][]TxIndex, 0)
	table := make(map[uint32]uint32)
	for i := uint32(0); i < txCount; i++ {
		root := unionSet.Find(i)
		index, ok := table[root]
		if !ok {
			table[root] = uint32(len(subGraphs))
			subGraphs = append(subGraphs, []TxIndex{TxIndex{int(i), scheduler.pendingTxns[i]}})
		} else {
			subGraphs[index] = append(subGraphs[index], TxIndex{int(i), scheduler.pendingTxns[i]})
		}
	}

	return subGraphs
}
