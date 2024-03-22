package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

// DependencyGraph methods
// Helpers for Kosaraju's algorithm
// dfs calculates the number of reachable vertices from a node
func (g *DependencyGraph) dfs(node int, visited []bool) int {
	visited[node] = true
	count := 1 // including itself
	for i, isEdge := range g.graph[node] {
		if isEdge && !visited[i] {
			count += g.dfs(i, visited) // recursively calculate reachable vertices
		}
	}
	return count
}

// CalculateMaxReachableSubgraphSize calculates the size of the maximum reachable subgraph from any vertex
func (g *DependencyGraph) CalculateMaxReachableSubgraphSize() int {
	g.lock.RLock()
	defer g.lock.RUnlock()

	maxSize := 0
	for i := range g.graph {
		visited := make([]bool, len(g.graph))
		size := g.dfs(i, visited)
		if size > maxSize {
			maxSize = size
		}
	}
	return maxSize
}

// isCyclicUtil is a recursive function used for DFS traversal and cycle detection.
func (g *DependencyGraph) isCyclicUtil(node int, visited []bool, recStack []bool) bool {
	if visited[node] == false {
		// Mark the current node as visited and enter the recursion stack
		visited[node] = true
		recStack[node] = true

		// Traverse all adjacent nodes
		for i, connected := range g.graph[node] {
			if connected {
				if !visited[i] && g.isCyclicUtil(i, visited, recStack) {
					return true
				} else if recStack[i] {
					return true
				}
			}
		}
	}
	// Remove the node from the recursion stack
	recStack[node] = false
	return false
}

// ComputeTransitiveClosure calculates the transitive closure of the graph
func (g *DependencyGraph) ComputeTransitiveClosure() [][]bool {
	size := len(g.graph)
	closure := make([][]bool, size)
	for i := range closure {
		closure[i] = make([]bool, size)
		copy(closure[i], g.graph[i])
	}
	for k := 0; k < size; k++ {
		for i := 0; i < size; i++ {
			for j := 0; j < size; j++ {
				closure[i][j] = closure[i][j] || (closure[i][k] && closure[k][j])
			}
		}
	}
	return closure
}

// RemoveRedundantEdges removes redundant edges from the graph
func (g *DependencyGraph) RemoveRedundantEdges() {
	g.lock.Lock()
	defer g.lock.Unlock()

	closure := g.ComputeTransitiveClosure()
	size := len(g.graph)

	for i := 0; i < size; i++ {
		for j := 0; j < size; j++ {
			if i != j && g.graph[i][j] && closure[i][j] {
				// Check if there exists any path i->...->j besides the direct edge i->j
				for k := 0; k < size; k++ {
					if k != i && k != j && closure[i][k] && closure[k][j] {
						g.graph[i][j] = false // Clip the redundant edge
						break
					}
				}
			}
		}
	}
}

// IsDAG checks if the DependencyGraph is a Directed Acyclic Graph (DAG)
func (g *DependencyGraph) IsDAG() bool {
	g.lock.RLock()
	defer g.lock.RUnlock()

	nodeCount := len(g.graph)
	visited := make([]bool, nodeCount)
	recStack := make([]bool, nodeCount)

	// Call helper function to detect cycles for each node
	for node := 0; node < nodeCount; node++ {
		if !visited[node] {
			if g.isCyclicUtil(node, visited, recStack) {
				return false
			}
		}
	}
	return true
}

func (dg *DependencyGraph) UpdateGraph(tx *Transaction, txIndex int, ff []bool, txs []Transaction) {
	dg.lock.Lock()
	defer dg.lock.Unlock()
	// Iterate through finishLine to check which transactions are completed
	for i, f := range ff {
		if !f {
			continue // Skip if the current transaction is incomplete
		}
		if i == txIndex {
			continue
		}
		conflict := false
		// Check if the write set of the completed transaction overlaps with the read/write set of the given transaction
		for _, writeAddress := range txs[i].WriteStateAddresses {
			for _, readAddress := range tx.ReadStateAddresses {
				if writeAddress == readAddress {
					dg.graph[txIndex][i] = true
					conflict = true
					break // Found overlap, set edge to true, then check the next transaction
				}
			}
			for _, writeAddressTx := range tx.WriteStateAddresses {
				if writeAddress == writeAddressTx {
					dg.graph[txIndex][i] = true
					conflict = true
					break // Found overlap, set edge to true, then check the next transaction
				}
			}
			if conflict == true {
				break
			}
		}
	}
}

func findExecutableTransactions(tdg [][]bool) []int {
	var executable []int

	// Check if each transaction does not depend on any other transactions (check each row in the graph)
	for txIndex, edges := range tdg {
		if tdg[txIndex][txIndex] == true {
			continue
		}
		isExecutable := true
		for _, depExists := range edges {
			if depExists {
				isExecutable = false // If there is a dependency, the transaction cannot be executed immediately
				break
			}
		}
		if isExecutable {
			executable = append(executable, txIndex)
		}
	}
	return executable
}

func (dg *DependencyGraph) RemoveTransaction(txIndex int) {
	dg.lock.Lock()
	defer dg.lock.Unlock()
	// Set all edges related to this transaction to false
	for i := range dg.graph {
		dg.graph[txIndex][i] = false // Remove dependencies of this transaction on other transactions
		dg.graph[i][txIndex] = false // Remove dependencies of other transactions on this transaction
	}
	dg.graph[txIndex][txIndex] = true
}

func occwsiExecuteTransaction(tx *Transaction, txs []Transaction, semaphore chan struct{}, sm *StateManager, dg *DependencyGraph, i int, fLine *finishLine) {

	semaphore <- struct{}{}

	snapshot := make(map[string]int)

	//sm.lock.RLock()
	for _, addr := range tx.ReadStateAddresses {
		snapshot[addr] = sm.stateMap[addr].Version
	}
	for _, addr := range tx.WriteStateAddresses {
		snapshot[addr] = sm.stateMap[addr].Version
	}
	//sm.lock.RUnlock()
	/*for _, addr := range append(tx.ReadStateAddresses, tx.WriteStateAddresses...) {
		_ = sm.stateMap[addr]
	}*/
	time.Sleep(time.Nanosecond * time.Duration(tx.ExecutionTime)) // Simulate transaction execution time
	conflict := false
	// Check if the version numbers have changed after execution
	sm.lock.RLock()
	for addr, version := range snapshot {
		currentVersion := sm.stateMap[addr].Version
		if currentVersion != version {
			conflict = true
			break
		}
	}
	sm.lock.RUnlock()
	// If version numbers haven't changed, the transaction executed successfully
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
		dg.UpdateGraph(tx, i, ff, txs)
		//fmt.Printf("Transaction %s executed successfully.\n", tx.TransactionHash)
		<-semaphore
	} else {
		<-semaphore
		//fmt.Printf("Transaction %s aborted due to conflict.\n", tx.TransactionHash)
	}
}

type finishLine struct {
	fs   []bool
	lock sync.RWMutex
}

func occWsi(blocks []Block, stateManager *StateManager, class string) {
	// Create output file
	outputFilePath := strconv.Itoa(thread) + class + "_wsi_execution_times.csv"
	outputFile, err := os.Create(outputFilePath)
	if err != nil {
		fmt.Printf("Error creating output file: %v\n", err)
		return
	}
	defer outputFile.Close()
	writer := csv.NewWriter(outputFile)
	defer writer.Flush()
	writer.Write([]string{"BlockNumber", "ExecutionTime(ms)", "ValidationTime(ms)"})

	// Record total execution time
	totalExecTime := time.Duration(0)
	totalValiTime := time.Duration(0)

	// Sequentially execute each block, but execute transactions within a block in parallel
	for _, block := range blocks {
		println("Packaging phase started:", block.BlockNumber)
		startTime := time.Now()
		semaphore := make(chan struct{}, thread) // Use global variable thread as parallelism degree
		tdg := NewDependencyGraph(len(block.Transactions))
		fLine := &finishLine{fs: make([]bool, len(block.Transactions))}
		var toExecute []int // Store the indices of transactions to be executed or re-executed

		// Initially mark all transactions as needing execution
		for i := range block.Transactions {
			toExecute = append(toExecute, i)
		}
		round := 0
		// Loop until all transactions are successfully executed
		for len(toExecute) > 0 {
			wg := sync.WaitGroup{}
			for _, i := range toExecute {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					occwsiExecuteTransaction(&block.Transactions[i], block.Transactions, semaphore, stateManager, tdg, i, fLine)
				}(i)
			}

			wg.Wait() // Wait for all attempted transactions in this round to complete
			round++
			toExecute = toExecute[:0] // Clear to collect indices of failed transactions again
			for i, done := range fLine.fs {
				if !done {
					toExecute = append(toExecute, i)
				}
			}
		}
		println(round)
		execTime := time.Since(startTime)
		totalExecTime += execTime
		//tdg.RemoveRedundantEdges()
		println(tdg.IsDAG())
		maxSize := tdg.CalculateMaxReachableSubgraphSize()
		println("max:", maxSize, "total:", len(block.Transactions))
		println("Validation phase started:", block.BlockNumber)
		// Validation phase: execute transactions in parallel based on the dependency graph
		startTime = time.Now()
		for {
			executable := findExecutableTransactions(tdg.graph)
			if len(executable) <= 1 {
				break // No more executable transactions, exit loop
			}
			var wg sync.WaitGroup
			for _, txIndex := range executable {
				wg.Add(1)
				go func(index int) {
					defer wg.Done()
					semaphore <- struct{}{} // Acquire semaphore
					// Execute transaction logic...
					for _, addr := range append(block.Transactions[index].ReadStateAddresses, block.Transactions[index].WriteStateAddresses...) {
						_ = stateManager.stateMap[addr]
					}
					time.Sleep(time.Nanosecond * time.Duration(block.Transactions[index].ExecutionTime)) // Simulate transaction execution time
					tdg.RemoveTransaction(index)                                                         // Remove completed transaction from the dependency graph
					<-semaphore
				}(txIndex)
			}
			wg.Wait() // Wait for all executable transactions in this round to complete
		}
		valiTime := time.Since(startTime)
		totalValiTime += valiTime
		writer.Write([]string{block.BlockNumber, fmt.Sprintf("%d", execTime.Milliseconds()), fmt.Sprintf("%d", valiTime.Milliseconds())})
	}
	// Record total execution time
	writer.Write([]string{"Total Execution Time", fmt.Sprintf("%d", totalExecTime.Milliseconds())})
	writer.Write([]string{"Total Validation Time", fmt.Sprintf("%d", totalValiTime.Milliseconds())})
}

// ContainsFalse checks if there are any false elements in a bool slice
func ContainsFalse(slice []bool) bool {
	for _, value := range slice {
		if !value { // If false is found
			return true // Return true
		}
	}
	return false // If no false is found, return false
}
