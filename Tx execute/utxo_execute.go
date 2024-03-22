package main

import (
	"encoding/csv"
	"fmt"
	"github.com/syndtr/goleveldb/leveldb"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const executetime = 25 * time.Microsecond
const delaytime = 200 * time.Microsecond

// VesselTX defines the structure of a transaction, including the block number.
type VesselTX struct {
	BlockNumber   string
	TransactionID string
	From          string
	To            string
}

// VTB defines the structure of a block, containing the block number and a slice of transactions.
type VTB struct {
	BlockNumber  string
	Transactions []VesselTX
}

// groupUTXByBlock groups transactions by block number.
func groupUTXByBlock(transactions []VesselTX) []VTB {
	var blocks []VTB
	blockMap := make(map[string]int) // Used to track the index of each block number in the blocks slice.

	for _, tx := range transactions {
		blockNumber := tx.BlockNumber
		if index, exists := blockMap[blockNumber]; exists {
			blocks[index].Transactions = append(blocks[index].Transactions, tx)
		} else {
			newBlock := VTB{
				BlockNumber:  blockNumber,
				Transactions: []VesselTX{tx},
			}
			blocks = append(blocks, newBlock)
			blockMap[blockNumber] = len(blocks) - 1
		}
	}

	sort.Slice(blocks, func(i, j int) bool {
		// Convert strings to integers for proper sorting.
		iBlockNumber, _ := strconv.Atoi(blocks[i].BlockNumber)
		jBlockNumber, _ := strconv.Atoi(blocks[j].BlockNumber)
		return iBlockNumber < jBlockNumber
	})
	return blocks
}

func vessel_parallel_execute() {
	rand.Seed(time.Now().UnixNano()) // Initialize random number seed.
	// Open the CSV file.
	file, err := os.Open("vessel.csv")
	defer file.Close()
	// Open or create the database.
	db, err := leveldb.OpenFile("../exp-init/leveldb", nil)
	if err != nil {
		panic(err)
	}
	// Create a CSV reader.
	reader := csv.NewReader(file)
	_, err = reader.Read() // Skip the header row.
	if err != nil {
		panic(err)
	}
	// Read the CSV file and create a slice of transactions.
	var transactions []VesselTX
	for {
		record, err := reader.Read()
		if err != nil {
			break // End of file or encountered an error.
		}
		tx := VesselTX{
			BlockNumber:   record[0],
			TransactionID: record[1],
			From:          record[2],
			To:            record[3],
		}
		transactions = append(transactions, tx)
	}
	// Group transactions by block number.
	blocks := groupUTXByBlock(transactions)
	// Initialize the vessel collection.
	vesselMap := sync.Map{}
	for _, tx := range transactions {
		value := generateRandomHash(32)
		err := db.Put([]byte(tx.From), []byte(value), nil)
		if err != nil {
			panic(err)
		}
		vesselMap.Store(tx.From, value)
	}
	totalExecTime := time.Duration(0)
	// Create the output file.
	outputFilePath := strconv.Itoa(thread) + "vessel_execution_times_pack.csv"
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		return
	}
	defer outputFile.Close()
	writer := csv.NewWriter(outputFile)
	defer writer.Flush()
	writer.Write([]string{"BlockNumber", "ExecutionTime(ms)"})
	semaphore := make(chan struct{}, thread) // Control concurrency.

	// Iterate over blocks to process each transaction.
	for _, block := range blocks {
		println("vessel pack", block.BlockNumber)
		startTime := time.Now()
		toExecute := make([]bool, len(block.Transactions)) // Store indices of transactions to be executed or re-executed.
		var mutex sync.RWMutex
		last := 0
		// Initially mark all transactions for execution.
		for {
			sum := 0
			finish := true
			wg := sync.WaitGroup{}
			for i, executed := range toExecute {
				if executed {
					continue
				}
				sum++
				finish = false
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					semaphore <- struct{}{} // Acquire semaphore.
					executeVTX(i, toExecute, block.Transactions, &mutex, db)
					<-semaphore // Release semaphore.
				}(i)
			}
			wg.Wait() // Wait for all goroutines to complete.
			if finish {
				break
			}
			if sum == last {
				break
			}
			last = sum
		}
		execTime := time.Since(startTime)
		totalExecTime += execTime
		writer.Write([]string{block.BlockNumber, fmt.Sprintf("%d", execTime.Milliseconds())})
	}

	// Record the total execution time.
	writer.Write([]string{"Total Execution Time", fmt.Sprintf("%d", totalExecTime.Milliseconds())})
	// Create the output file.
	outputFilePath = strconv.Itoa(thread) + "vessel_execution_times_val.csv"
	outputFile, err = os.Create(outputFilePath)
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		return
	}
	defer outputFile.Close()

	writer = csv.NewWriter(outputFile)
	defer writer.Flush()
	writer.Write([]string{"BlockNumber", "ExecutionTime(ms)"})
	totalExecTime = time.Duration(0)
	// Iterate over blocks to process each transaction.
	for _, block := range blocks {
		println("vessel validate", block.BlockNumber)
		// Randomly select transactions to retain.
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(block.Transactions), func(i, j int) {
			block.Transactions[i], block.Transactions[j] = block.Transactions[j], block.Transactions[i]
		})
		numToKeep := len(block.Transactions) / 3 // Retain one-third.
		selectedTransactions := block.Transactions[:numToKeep]
		startTime := time.Now()
		tdg := NewDependencyGraph(len(selectedTransactions))
		tdg.graph = generateDependencyGraph(selectedTransactions)
		var wg sync.WaitGroup
		for {
			executable := findExecutableTransactions(tdg.graph)
			if len(executable) <= 1 {
				break // No more executable transactions, exit loop.
			}
			for _, txIndex := range executable {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					semaphore <- struct{}{} // Acquire semaphore.
					err := db.Delete([]byte(selectedTransactions[index].From), nil)
					if err != nil {
						panic(err)
					}
					value := generateRandomHash(32)
					err = db.Put([]byte(selectedTransactions[index].To), []byte(value), nil)
					if err != nil {
						panic(err)
					}
					time.Sleep(executetime)
					tdg.RemoveTransaction(index) // Remove completed transactions from the dependency graph.
					<-semaphore                  // Release semaphore.
				}(txIndex)
			}
			wg.Wait() // Wait for all executable transactions of this round to complete.
		}
		execTime := time.Since(startTime)
		totalExecTime += execTime
		writer.Write([]string{block.BlockNumber, fmt.Sprintf("%d", execTime.Milliseconds())})
	}
	// Record the total execution time.
	writer.Write([]string{"Total Execution Time", fmt.Sprintf("%d", totalExecTime.Milliseconds())})
}

func vessel_serial_execute() {
	rand.Seed(time.Now().UnixNano()) // Initialize random number seed.
	// Open the CSV file.
	file, err := os.Open("vessel.csv")
	defer file.Close()
	// Open or create the database.
	db, err := leveldb.OpenFile("../exp-init/leveldb", nil)
	defer db.Close()
	if err != nil {
		panic(err)
	}
	// Create a CSV reader.
	reader := csv.NewReader(file)
	_, err = reader.Read() // Skip the header row.
	if err != nil {
		panic(err)
	}
	// Read the CSV file and create a slice of transactions.
	var transactions []VesselTX
	for {
		record, err := reader.Read()
		if err != nil {
			break // End of file or encountered an error.
		}
		tx := VesselTX{
			BlockNumber:   record[0],
			TransactionID: record[1],
			From:          record[2],
			To:            record[3],
		}
		transactions = append(transactions, tx)
	}
	// Group transactions by block number.
	blocks := groupUTXByBlock(transactions)
	// Initialize the vessel collection.
	for _, tx := range transactions {
		value := generateRandomHash(32)
		err := db.Put([]byte(tx.From), []byte(value), nil)
		if err != nil {
			panic(err)
		}
	}
	// Create the output file.
	outputFilePath := "serial_vessel_execution_times.csv"
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		return
	}
	defer outputFile.Close()
	writer := csv.NewWriter(outputFile)
	defer writer.Flush()
	writer.Write([]string{"BlockNumber", "ExecutionTime(ms)"})
	totalExecTime := time.Duration(0)
	// Iterate over blocks to process each transaction.
	for _, block := range blocks {
		startTime := time.Now()
		for i := range block.Transactions {
			tx := block.Transactions[i] // Create a local variable copy for the current loop iteration.
			err = db.Delete([]byte(tx.From), nil)
			if err != nil {
				panic(err)
			}
			value := generateRandomHash(32)
			err = db.Put([]byte(tx.To), []byte(value), nil)
			if err != nil {
				panic(err)
			}
			time.Sleep(executetime)
		}
		execTime := time.Since(startTime)
		totalExecTime += execTime
		writer.Write([]string{block.BlockNumber, fmt.Sprintf("%d", execTime.Milliseconds())})
	}
	// Record the total execution time.
	writer.Write([]string{"Total Execution Time", fmt.Sprintf("%d", totalExecTime.Milliseconds())})
}

func generateRandomHash(length int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	b := make([]rune, length)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
func splitTransactionID(txID string) []string {
	// Use the Split method to divide the string using "_" as the separator.
	parts := strings.Split(txID, "_")
	if len(parts) != 3 {
		fmt.Println("Unexpected format of transaction ID:", txID)
		return nil
	}
	return parts // Return a slice containing transactionHash, from/to, tokenAddress.
}

// Modify the executeVTX function to add transactions needing retry to the retry queue instead of waiting directly for retry.
func executeVTX(index int, toExecute []bool, txs []VesselTX, lock *sync.RWMutex, db *leveldb.DB) {
	fromParts := splitTransactionID(txs[index].From)

	retry := false
	lock.RLock()
	for i, f := range toExecute {
		if f {
			continue
		}
		otherTxID := txs[i].TransactionID
		otherToParts := splitTransactionID(txs[i].To)
		if fromParts[2] == otherToParts[2] && fromParts[1] == otherToParts[1] {
			if txs[index].TransactionID != otherTxID {
				retry = true
				break
			}
		}
	}
	lock.RUnlock()
	if retry {
		return
	}
	lock.Lock()
	toExecute[index] = true
	lock.Unlock()

	err := db.Delete([]byte(txs[index].From), nil)
	if err != nil {
		panic(err)
	}
	value := generateRandomHash(32)
	err = db.Put([]byte(txs[index].To), []byte(value), nil)
	if err != nil {
		panic(err)
	}
	time.Sleep(executetime)
}

func canAddEdgeWithoutCycle(from, to int, graph [][]bool, n int) bool {
	visited := make([]bool, n)
	var dfs func(int) bool
	dfs = func(node int) bool {
		if node == to {
			return true
		}
		visited[node] = true
		for next := 0; next < n; next++ {
			if graph[node][next] && !visited[next] {
				if dfs(next) {
					return true
				}
			}
		}
		return false
	}
	// Check if adding this edge would result in a path from 'to' to 'from', indicating the formation of a cycle.
	return !dfs(from)
}

func generateDependencyGraph(selectedTransactions []VesselTX) [][]bool {
	n := len(selectedTransactions)
	dependencyGraph := make([][]bool, n)
	for i := range dependencyGraph {
		dependencyGraph[i] = make([]bool, n)
	}

	for i, txA := range selectedTransactions {
		for j, txB := range selectedTransactions {
			if txA.TransactionID == txB.TransactionID && txA.To == txB.From {
				// Check if adding this edge would result in a cycle in the graph.
				if canAddEdgeWithoutCycle(j, i, dependencyGraph, n) {
					dependencyGraph[j][i] = true // Edge from txB to txA exists.
				}
			}
		}
	}
	return dependencyGraph
}
