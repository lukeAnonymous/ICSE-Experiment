package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

// deoccExecuteTransaction simulates the execution of a transaction and checks for conflicts.
func deoccExecuteTransaction(tx *Transaction, txs []Transaction, semaphore chan struct{}, sm *StateManager, i int, fLine *finishLine) {

	semaphore <- struct{}{}

	snapshot := make(map[string]int)

	for _, addr := range tx.ReadStateAddresses {
		snapshot[addr] = sm.stateMap[addr].Version
	}
	for _, addr := range tx.WriteStateAddresses {
		snapshot[addr] = sm.stateMap[addr].Version
	}

	time.Sleep(time.Nanosecond * time.Duration(tx.ExecutionTime)) // Simulate transaction execution time
	conflict := false

	sm.lock.RLock()
	for addr, version := range snapshot {
		currentVersion := sm.stateMap[addr].Version
		if currentVersion != version {
			conflict = true
			break
		}
	}
	sm.lock.RUnlock()

	if !conflict {
		sm.lock.Lock()
		for _, addr := range tx.WriteStateAddresses {
			sm.stateMap[addr].Value++
			sm.stateMap[addr].Version++
		}
		sm.lock.Unlock()
		fLine.lock.Lock()
		fLine.fs[i] = true
		ff := make([]bool, len(txs))
		copy(ff, fLine.fs)
		fLine.lock.Unlock()
		<-semaphore
	} else {
		<-semaphore
	}
}

// BuildConflictGraph constructs the conflict graph representation of transactions.
func BuildConflictGraph(transactions []Transaction) [][]bool {
	n := len(transactions)
	conflictGraph := make([][]bool, n)
	for i := range conflictGraph {
		conflictGraph[i] = make([]bool, n)
	}
	for i, txA := range transactions {
		for j, txB := range transactions {
			if i == j {
				continue
			}
			for _, readAddr := range txA.ReadStateAddresses {
				for _, writeAddr := range txB.WriteStateAddresses {
					if readAddr == writeAddr {
						conflictGraph[i][j] = true
						break
					}
				}
				if conflictGraph[i][j] {
					break
				}
			}
			if conflictGraph[i][j] {
				continue
			}
			for _, writeAddrA := range txA.WriteStateAddresses {
				for _, writeAddrB := range txB.WriteStateAddresses {
					if writeAddrA == writeAddrB {
						conflictGraph[i][j] = true
						break
					}
				}
				if conflictGraph[i][j] {
					break
				}
			}
		}
	}
	return conflictGraph
}

// deOCC simulates the execution of transactions using the deOCC algorithm.
func deOCC(blocks []Block, stateManager *StateManager, class string) {
	outputFilePath := strconv.Itoa(thread) + class + "_deocc_execution_times.csv"
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		return
	}
	defer outputFile.Close()
	writer := csv.NewWriter(outputFile)
	defer writer.Flush()
	writer.Write([]string{"BlockNumber", "ExecutionTime(ms)", "ValidationTime(ms)", "needRW"})

	totalExecTime := time.Duration(0)
	totalValiTime := time.Duration(0)

	for _, block := range blocks {
		println("Start of packaging phase:", block.BlockNumber)
		startTime := time.Now()
		semaphore := make(chan struct{}, thread)
		fLine := &finishLine{fs: make([]bool, len(block.Transactions))}
		var toExecute []int
		for i := range block.Transactions {
			toExecute = append(toExecute, i)
		}
		for len(toExecute) > 0 {
			wg := sync.WaitGroup{}
			for _, i := range toExecute {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					deoccExecuteTransaction(&block.Transactions[i], block.Transactions, semaphore, stateManager, i, fLine)
				}(i)
			}
			wg.Wait()
			toExecute = toExecute[:0]
			for i, done := range fLine.fs {
				if !done {
					toExecute = append(toExecute, i)
				}
			}
		}
		tdg, sum := buildtdg(block.Transactions)
		maxSize := tdg.CalculateMaxReachableSubgraphSize()
		println("max:", maxSize, "total:", len(block.Transactions))
		execTime := time.Since(startTime)
		totalExecTime += execTime
		println(sum)
		println("Start of validation phase:", block.BlockNumber)
		startTime = time.Now()
		for {
			executable := findExecutableTransactions(tdg.graph)
			if len(executable) <= 1 {
				break
			}
			var wg sync.WaitGroup
			semaphore := make(chan struct{}, thread)
			for _, txIndex := range executable {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					semaphore <- struct{}{}
					for _, addr := range append(block.Transactions[index].ReadStateAddresses, block.Transactions[index].WriteStateAddresses...) {
						_ = stateManager.stateMap[addr]
					}
					time.Sleep(time.Nanosecond * time.Duration(block.Transactions[index].ExecutionTime))
					tdg.RemoveTransaction(index)
					<-semaphore
				}(txIndex)
			}
			wg.Wait()
		}
		valiTime := time.Since(startTime)
		totalValiTime += valiTime
		writer.Write([]string{block.BlockNumber, fmt.Sprintf("%d", execTime.Milliseconds()), fmt.Sprintf("%d", valiTime.Milliseconds()), fmt.Sprintf("%d", sum)})
	}
	writer.Write([]string{"Total Execution Time", fmt.Sprintf("%d", totalExecTime.Milliseconds())})
	writer.Write([]string{"Total Validation Time", fmt.Sprintf("%d", totalValiTime.Milliseconds())})
}

// buildtdg constructs the transaction dependency graph and determines the maximum reachable subgraph size.
func buildtdg(transactions []Transaction) (*DependencyGraph, int) {
	cg := BuildConflictGraph(transactions)
	tdg := NewDependencyGraph(len(transactions))
	in := make([]bool, len(transactions))
	remainingTransactions := make([]Transaction, len(transactions))
	copy(remainingTransactions, transactions)
	for {
		fvs := GreedySelectVerticesForFVS(cg)
		finish := true
		for i, _ := range fvs {
			if fvs[i] {
				finish = false
			}
		}
		cgCopy := make([][]bool, len(cg))
		for i := range cg {
			cgCopy[i] = make([]bool, len(cg[i]))
			copy(cgCopy[i], cg[i])
		}
		for i, _ := range fvs {
			if fvs[i] {
				removeVertexAndEdges(cgCopy, i)
			}
		}
		for i := 0; i < len(transactions); i++ {
			for j := 0; j < len(transactions); j++ {
				if cgCopy[i][j] == true {
					tdg.graph[i][j] = true
				}
			}
		}
		for i, _ := range fvs {
			if !fvs[i] && !in[i] {
				for j, _ := range in {
					if in[j] {
						tdg.graph[i][j] = true
					}
				}
			}
		}
		for i, _ := range fvs {
			if !fvs[i] && !in[i] {
				in[i] = true
				removeVertexAndEdges(cg, i)
			}
		}
		if finish {
			break
		}
	}
	tdg.RemoveRedundantEdges()
	var totalExecTime int64
	for _, t := range transactions {
		totalExecTime += t.ExecutionTime
	}
	threshold := totalExecTime / 20
	partitions := partitionTDG(transactions, tdg.graph, threshold)
	partitionIDMap := assignPartitionIDs(transactions, partitions)
	sum := removeInterPartitionEdges(tdg.graph, transactions, partitionIDMap)
	return tdg, sum
}

// dfs performs a depth-first search to calculate the total execution time of connected components.
func dfs(currentNode int, visited []bool, graph [][]bool, transactions []Transaction) int64 {
	visited[currentNode] = true
	totalExecTime := transactions[currentNode].ExecutionTime

	for nextNode, exists := range graph[currentNode] {
		if exists && !visited[nextNode] {
			totalExecTime += dfs(nextNode, visited, graph, transactions)
		}
	}
	return totalExecTime
}

// canRemoveEdge determines if an edge can be removed without violating the execution time threshold.
func canRemoveEdge(transactions []Transaction, matrix [][]bool, targetExecTime int64, i int, j int) bool {
	matrix[i][j] = false

	visited := make([]bool, len(transactions))
	execTimeAfterRemoval := dfs(i, visited, matrix, transactions)

	matrix[i][j] = true

	return execTimeAfterRemoval >= targetExecTime
}

// allVisited checks if all nodes have been visited.
func allVisited(visited []bool) bool {
	for _, v := range visited {
		if !v {
			return false
		}
	}
	return true
}

// partitionTDG partitions the transaction dependency graph into smaller subgraphs.
func partitionTDG(transactions []Transaction, graph [][]bool, threshold int64) [][]Transaction {
	n := len(transactions)
	visited := make([]bool, n)
	var partitions [][]Transaction
	var currPartition []Transaction

	for i := 0; i < n; i++ {
		if !visited[i] {
			if currWeight(currPartition)+transactions[i].ExecutionTime > threshold {
				partitions = append(partitions, currPartition)
				currPartition = []Transaction{}
			}
			visited[i] = true
			currPartition = append(currPartition, transactions[i])

			for j := 0; j < n; j++ {
				if graph[i][j] && !visited[j] {
					if currWeight(currPartition)+transactions[j].ExecutionTime > threshold {
						partitions = append(partitions, currPartition)
						currPartition = []Transaction{}
					}
					visited[j] = true
					currPartition = append(currPartition, transactions[j])
				}
			}
		}
	}
	if len(currPartition) > 0 {
		partitions = append(partitions, currPartition)
	}
	return partitions
}

// currWeight calculates the total weight of the current partition.
func currWeight(partition []Transaction) int64 {
	var weight int64 = 0
	for _, transaction := range partition {
		weight += transaction.ExecutionTime
	}
	return weight
}

// assignPartitionIDs assigns partition identifiers to transactions.
func assignPartitionIDs(transactions []Transaction, partitions [][]Transaction) map[string]int {
	partitionIDMap := make(map[string]int)
	for i, partition := range partitions {
		for _, transaction := range partition {
			partitionIDMap[transaction.TransactionHash] = i
		}
	}
	return partitionIDMap
}

// removeInterPartitionEdges removes edges between transactions in different partitions.
func removeInterPartitionEdges(graph [][]bool, transactions []Transaction, partitionIDMap map[string]int) int {
	sum := 0
	n := len(transactions)
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if graph[i][j] && partitionIDMap[transactions[i].TransactionHash] != partitionIDMap[transactions[j].TransactionHash] {
				graph[i][j] = false
				for _, _ = range transactions[j].WriteStateAddresses {
					sum++
				}
			}
		}
	}
	return sum
}
