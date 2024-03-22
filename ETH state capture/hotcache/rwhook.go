package hotcache

import (
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

type void struct{}
type ExecTime struct {
	Addr    common.Hash
	Timedur time.Duration
}

type RWRecorder struct {
	//statedb *StateDB

	txID common.Hash

	txIndex int

	RWChan         chan *RWS
	TxExecTime     chan ExecTime
	NowBLKNUM      uint64
	NowTxHash      common.Hash
	NowTxToAddress common.Address
}
type RWS struct {
	Address    *common.Address
	BlkNum     uint64
	Slot_key   uint256.Int
	Slot_value []byte
	IsRead     bool
	NowTxHash  common.Hash
	NowBLKNUM  uint64
}

func (h *RWRecorder) AddTxID(hash common.Hash, txIndex int) {
	h.txID = hash
	h.txIndex = txIndex
}

func (h *RWRecorder) GetTxID() common.Hash {
	return h.txID
}

func (hc *Hotcache) NewRWHook() *RWRecorder {
	rwr := &RWRecorder{

		txID:       common.Hash{},
		txIndex:    0,
		RWChan:     make(chan *RWS, 999999999),
		TxExecTime: make(chan ExecTime, 1000000),
	}
	//go insert_map_GoRoutine(rwr)

	return rwr

}
