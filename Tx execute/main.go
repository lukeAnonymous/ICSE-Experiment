package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const thread = 64
const filePath_all = "transactions.csv"
const filePath_token = "transactions_token.csv"
const filePath_without_token = "transactions_without_token.csv"

// Transaction defines the structure of a transaction
type Transaction struct {
	BlockNumber         string   // Block number
	TransactionHash     string   // Transaction hash
	ReadStateAddresses  []string // Addresses read by the transaction
	WriteStateAddresses []string // Addresses written to by the transaction
	ExecutionTime       int64    // Execution time of the transaction in nanoseconds
}

// StateValue defines the structure for a state value, including an integer value and a version number
type StateValue struct {
	Value   int
	Version int
}

// BlockExecutionResult defines the result of block execution
type BlockExecutionResult struct {
	BlockNumber string
	ExecTime    time.Duration
}

type StateManager struct {
	stateMap map[string]*StateValue
	lock     sync.RWMutex
}

type DependencyGraph struct {
	graph [][]bool
	lock  sync.RWMutex
}
type Block struct {
	BlockNumber  string
	Transactions []Transaction
}

func NewStateManager(transactions []Transaction) *StateManager {
	stateManager := &StateManager{stateMap: make(map[string]*StateValue)}
	for _, tx := range transactions {
		for _, addr := range append(tx.ReadStateAddresses, tx.WriteStateAddresses...) {
			if _, exists := stateManager.stateMap[addr]; !exists {
				stateManager.stateMap[addr] = &StateValue{Value: 0, Version: 0}
			}
		}
	}
	return stateManager
}

func NewDependencyGraph(n int) *DependencyGraph {
	Graph := make([][]bool, n)
	for i := range Graph {
		Graph[i] = make([]bool, n)
	}
	return &DependencyGraph{graph: Graph}
}

func main() {

	transactions, err := readCSV(filePath_all)
	if err != nil {
		fmt.Printf("Error reading CSV file: %v\n", err)
		return
	}
	stateManager := NewStateManager(transactions)
	// Group transactions by block number
	blocks := groupTransactionsByBlock(transactions)
	// Serial execution of contract transactions
	serial(blocks, stateManager.stateMap, "all")
	// Parallel execution of contract transactions with OCCWSI
	stateManager = NewStateManager(transactions)
	occWsi(blocks, stateManager, "all")
	// Parallel execution of contract transactions with DEOCC
	stateManager = NewStateManager(transactions)
	deOCC(blocks, stateManager, "all")

	// Execute non-token contract transactions
	transactions, err = readCSV(filePath_without_token)
	if err != nil {
		fmt.Printf("Error reading CSV file: %v\n", err)
		return
	}
	stateManager = NewStateManager(transactions)
	blocks = groupTransactionsByBlock(transactions)
	serial(blocks, stateManager.stateMap, "without_token")
	stateManager = NewStateManager(transactions)
	occWsi(blocks, stateManager, "without_token")
	stateManager = NewStateManager(transactions)
	deOCC(blocks, stateManager, "without_token")

	// Execute token contract transactions
	transactions, err = readCSV(filePath_token)
	if err != nil {
		fmt.Printf("Error reading CSV file: %v\n", err)
		return
	}
	stateManager = NewStateManager(transactions)
	blocks = groupTransactionsByBlock(transactions)
	serial(blocks, stateManager.stateMap, "token")
	stateManager = NewStateManager(transactions)
	occWsi(blocks, stateManager, "token")
	stateManager = NewStateManager(transactions)
	deOCC(blocks, stateManager, "token")

	// Serial execution of vessel transactions
	vessel_serial_execute()
	// Parallel execution of vessel transactions
	vessel_parallel_execute()

}

func serial(blocks []Block, stateMap map[string]*StateValue, class string) {
	// Create output file
	outputFilePath := class + "serial_execution_times.csv"
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		return
	}
	defer outputFile.Close()
	writer := csv.NewWriter(outputFile)
	defer writer.Flush()
	writer.Write([]string{"BlockNumber", "ExecutionTime(ms)"})

	// Record total execution time
	totalExecTime := time.Duration(0)

	// Execute each block in sequence
	for _, block := range blocks {
		startTime := time.Now()
		executeBlockSerial(block.Transactions, stateMap)
		execTime := time.Since(startTime)
		totalExecTime += execTime
		writer.Write([]string{block.BlockNumber, fmt.Sprintf("%d", execTime.Milliseconds())})
		println(block.BlockNumber)
	}
	// Record total execution time
	writer.Write([]string{"Total Execution Time", fmt.Sprintf("%d", totalExecTime.Milliseconds())})
}

func executeBlockSerial(txs []Transaction, stateMap map[string]*StateValue) {
	for _, tx := range txs {
		for _, addr := range tx.WriteStateAddresses {
			stateMap[addr].Value++
		}
		for _, addr := range tx.ReadStateAddresses {
			_ = stateMap[addr].Value
		}
		time.Sleep(time.Nanosecond * time.Duration(tx.ExecutionTime)) // Simulate transaction execution time
	}
}

func groupTransactionsByBlock(transactions []Transaction) []Block {
	var blocks []Block
	blockMap := make(map[string]int) // Tracks the index of each block number in the blocks slice

	for _, tx := range transactions {
		blockNumber := tx.BlockNumber
		if index, exists := blockMap[blockNumber]; exists {
			// If the block already exists, add the transaction to it
			blocks[index].Transactions = append(blocks[index].Transactions, tx)
		} else {
			// If the block does not exist, create a new block and add it to the slice
			newBlock := Block{
				BlockNumber:  blockNumber,
				Transactions: []Transaction{tx},
			}
			blocks = append(blocks, newBlock)
			blockMap[blockNumber] = len(blocks) - 1 // Update index mapping
		}
	}

	// Sort the blocks slice as needed to ensure correct order
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].BlockNumber < blocks[j].BlockNumber
	})

	return blocks
}

func readCSV(filePath string) ([]Transaction, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	var transactions []Transaction

	// Skip the header row
	reader.Read()

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		// Fix the processing of readStateAddresses and writeStateAddresses
		readStateAddresses := strings.Split(strings.TrimSpace(record[3]), "~")
		if len(readStateAddresses) > 0 && readStateAddresses[len(readStateAddresses)-1] == "" {
			readStateAddresses = readStateAddresses[:len(readStateAddresses)-1]
		}

		writeStateAddresses := strings.Split(strings.TrimSpace(record[4]), "~")
		if len(writeStateAddresses) > 0 && writeStateAddresses[len(writeStateAddresses)-1] == "" {
			writeStateAddresses = writeStateAddresses[:len(writeStateAddresses)-1]
		}

		// Fix the reading of ExecutionTime, first parsing as float, then converting to int64
		execTimeString := strings.TrimSpace(record[5])
		execTimeFloat, err := strconv.ParseFloat(execTimeString, 64)
		if err != nil {
			return nil, err
		}
		execTime := int64(execTimeFloat)

		transactions = append(transactions, Transaction{
			BlockNumber:         record[0],
			TransactionHash:     strings.TrimSpace(record[1]),
			ReadStateAddresses:  readStateAddresses,
			WriteStateAddresses: writeStateAddresses,
			ExecutionTime:       execTime,
		})
	}
	return transactions, nil
}
