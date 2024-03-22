package fff

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/eth/ethconfig"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/hotcache"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/metrics"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/params"
)

const (
	clientIdentifier    = "geth" // Client identifier to advertise over the network
	bodyCacheLimit      = 256
	blockCacheLimit     = 256
	receiptsCacheLimit  = 32
	txLookupCacheLimit  = 1024
	maxFutureBlocks     = 256
	maxTimeFutureBlocks = 30
	TriesInMemory       = 128
)

var emptyHash = common.Hash{}

type ethstatsConfig struct {
	URL string `toml:",omitempty"`
}

type gethConfig struct {
	Eth      ethconfig.Config
	Node     node.Config
	Ethstats ethstatsConfig
	Metrics  metrics.Config
}

func defaultNodeConfig() node.Config {
	cfg := node.DefaultConfig
	cfg.Name = clientIdentifier
	cfg.Version = params.VersionWithCommit("", "")
	cfg.HTTPModules = append(cfg.HTTPModules, "eth")
	cfg.WSModules = append(cfg.WSModules, "eth")
	cfg.IPCPath = "geth.ipc"
	return cfg
}

func removeDuplicate(arr []string) []string {
	resArr := make([]string, 0)
	tmpMap := make(map[string]interface{})
	for _, val := range arr {
		if _, ok := tmpMap[val]; !ok {
			resArr = append(resArr, val)
			tmpMap[val] = nil
		}
	}
	return resArr
}

func makeConfigNode(cfgDataDir string) (*node.Node, gethConfig) {
	// Load defaults.
	cfg := gethConfig{
		Eth:     ethconfig.Defaults,
		Node:    defaultNodeConfig(),
		Metrics: metrics.DefaultConfig,
	}
	cfg.Node.DataDir = cfgDataDir
	stack, err := node.New(&cfg.Node)
	if err != nil {
		utils.Fatalf("Failed to create the protocol stack: %v", err)
	}
	cfg.Eth.DatabaseCache = 2048
	cfg.Eth.TrieCleanCache = 2048
	cfg.Eth.TrieDirtyCache = 2048
	cfg.Eth.NoPruning = false
	cfg.Eth.SnapshotCache = 0
	cfg.Eth.SnapshotCache = 0 //默认
	cfg.Eth.NetworkId = 1
	cfg.Eth.Genesis = core.DefaultGenesisBlock()
	return stack, cfg
}

func writeTestLogs(bc *core.BlockChain, filename string, hotcache_ *hotcache.Hotcache) {
	file, err := os.Create(filename + ".csv")
	if err != nil {
		fmt.Println(err)
	}

	writer := csv.NewWriter(file)
	writer.Comma = ','
	defer writer.Flush()
	headline := []string{"BlockNumber", "TxHash", "InvokeAddress", "ReadStateSlot", "WriteStateSlot"}
	writer.Write(headline)
	t := time.NewTicker(5 * time.Second)
	var nowTxHash common.Hash
	var InvokeAddressList []string
	var ReadSlotList []string
	var WriteSlotList []string

	for {
		select {

		case RW := <-hotcache_.Recorder.RWChan:
			{

				if nowTxHash == emptyHash {
					nowTxHash = RW.NowTxHash
				}
				if nowTxHash != RW.NowTxHash {
					InvokeAddressList = removeDuplicate(InvokeAddressList)
					ReadSlotList = removeDuplicate(ReadSlotList)
					WriteSlotList = removeDuplicate(WriteSlotList)

					var InvokeAddressString bytes.Buffer
					for _, v := range InvokeAddressList {
						InvokeAddressString.WriteString(v + "~")
					}
					var ReadAddressString bytes.Buffer
					for _, v := range ReadSlotList {
						ReadAddressString.WriteString(v + "~")
					}
					var WriteAddressString bytes.Buffer
					for _, v := range WriteSlotList {
						WriteAddressString.WriteString(v + "~")
					}
					data := []string{strconv.FormatUint(RW.NowBLKNUM, 10), RW.NowTxHash.String(), InvokeAddressString.String(), ReadAddressString.String(), WriteAddressString.String()}
					writer.Write(data)

					nowTxHash = RW.NowTxHash
					InvokeAddressList = nil
					ReadSlotList = nil
					WriteSlotList = nil
				}
				InvokeAddressList = append(InvokeAddressList, RW.Address.String())
				if RW.IsRead {
					ReadSlotList = append(ReadSlotList, RW.Address.Hex()+RW.Slot_key.String())
				} else {
					WriteSlotList = append(WriteSlotList, RW.Address.Hex()+RW.Slot_key.String())
				}

			}

		case <-t.C:
			{
				writer.Flush()
				log.Info("Flushed into disk!")
			}

		}
	}
}

func writeTestLogsExecTime(bc *core.BlockChain, filename string, hotcache_ *hotcache.Hotcache) {
	file, err := os.Create(filename + "ExecTime.csv")
	if err != nil {
		fmt.Println(err)
	}

	writer := csv.NewWriter(file)
	writer.Comma = ','
	defer writer.Flush()
	headline := []string{"TxHash", "ExecTime(ns)"}
	writer.Write(headline)
	t := time.NewTicker(5 * time.Second)

	for {
		select {

		case RW := <-hotcache_.Recorder.TxExecTime:
			{

				data := []string{RW.Addr.Hex(), strconv.FormatInt(RW.Timedur.Nanoseconds(), 10)}
				writer.Write(data)

			}

		case <-t.C:
			{
				writer.Flush()
				log.Info("Flushed into disk Exectime!")
			}

		}
	}
}

func Gendata(startC uint64, endC uint64, writeTmp_db *ethdb.Database, read_db *ethdb.Database, filename string, cfgDataDir string) {
	stack, config := makeConfigNode(cfgDataDir)

	chainConfig, _, _ := core.SetupGenesisBlockWithOverride(*writeTmp_db, config.Eth.Genesis, config.Eth.OverrideTerminalTotalDifficulty, config.Eth.OverrideTerminalTotalDifficultyPassed)
	cacheConfig := &core.CacheConfig{
		Start_from:          startC - 1,
		TrieCleanLimit:      config.Eth.TrieCleanCache,
		TrieCleanJournal:    stack.ResolvePath(config.Eth.TrieCleanCacheJournal),
		TrieCleanRejournal:  config.Eth.TrieCleanCacheRejournal,
		TrieCleanNoPrefetch: true,
		TrieDirtyLimit:      config.Eth.TrieDirtyCache,
		TrieDirtyDisabled:   false,
		TrieTimeLimit:       config.Eth.TrieTimeout,
		SnapshotLimit:       0,
		Preimages:           false,

		HASize: 1,
	}
	vmConfig := vm.Config{
		EnablePreimageRecording: config.Eth.EnablePreimageRecording,
	}

	bc, ffff, err := core.NewBlockChainHotNni(*writeTmp_db, cacheConfig, chainConfig, ethconfig.CreateConsensusEngine(stack, chainConfig, &config.Eth.Ethash, config.Eth.Miner.Notify,
		config.Eth.Miner.Noverify, *writeTmp_db), vmConfig, func(header *types.Header) bool { return false }, &config.Eth.TxLookupLimit)

	if err != nil {
		log.Error("create blockchain err : %v", 1, err)
	}
	var chain_segment types.Blocks

	start_all := time.Now()

	go writeTestLogs(bc, filename, ffff)
	go writeTestLogsExecTime(bc, filename, ffff)

	for index := startC; index <= endC; index++ {

		blockHash := rawdb.ReadCanonicalHash(*read_db, index)
		block := rawdb.ReadBlock(*read_db, blockHash, index)
		chain_segment = append(chain_segment, block)
		ffff.Recorder.NowBLKNUM = block.NumberU64()
		if _, err := bc.InsertChain(chain_segment); err != nil {
			log.Error("ERR insert chain")
		}

		chain_segment = nil

		if index%100 == 0 {
			log.Info("Prcessed", "blk num", index, "total timeused", time.Since(start_all).Seconds())
		}

	}
}
