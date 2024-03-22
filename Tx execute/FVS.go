package main

// Global variables for Tarjan's algorithm.
var index int
var stack []int
var onStack map[int]bool
var indices map[int]int
var lowLink map[int]int
var scc [][]int

// strongConnect performs the strong connectivity check for a given vertex.
func strongConnect(v int, cg [][]bool) {
	indices[v] = index
	lowLink[v] = index
	index++
	stack = append(stack, v)
	onStack[v] = true

	for w, connected := range cg[v] {
		if !connected {
			continue
		}
		if indices[w] == -1 {
			strongConnect(w, cg)
			lowLink[v] = min(lowLink[v], lowLink[w])
		} else if onStack[w] {
			lowLink[v] = min(lowLink[v], indices[w])
		}
	}

	if lowLink[v] == indices[v] {
		var component []int
		for {
			w := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			onStack[w] = false
			component = append(component, w)
			if w == v {
				break
			}
		}
		scc = append(scc, component)
	}
}

// TarjanSCC finds all strongly connected components in a graph using Tarjan's algorithm.
func TarjanSCC(cg [][]bool) [][]int {
	index = 0
	stack = nil
	onStack = make(map[int]bool)
	indices = make(map[int]int)
	lowLink = make(map[int]int)
	scc = nil

	for v := range cg {
		indices[v] = -1
	}

	for v := range cg {
		if indices[v] == -1 {
			strongConnect(v, cg)
		}
	}

	return scc
}

// min returns the minimum of two integers.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// GreedySelectVerticesForFVS finds a minimal Feedback Vertex Set using a greedy approach.
func GreedySelectVerticesForFVS(cg [][]bool) []bool {
	// Create a deep copy of cg.
	cgCopy := make([][]bool, len(cg))
	for i := range cg {
		cgCopy[i] = make([]bool, len(cg[i]))
		copy(cgCopy[i], cg[i])
	}

	sccs := TarjanSCC(cgCopy) // Find all SCCs using Tarjan's algorithm.
	fvs := make([]bool, len(cgCopy))

	for _, scc := range sccs {
		// Create a set of vertices to check.
		verticesToCheck := make(map[int]bool)
		for _, v := range scc {
			verticesToCheck[v] = true
		}

		for len(verticesToCheck) > 0 {
			// Calculate degrees for each vertex.
			degrees, outDegrees := calculateDegrees(cgCopy, verticesToCheck)

			// Select a vertex with the maximum degree for pruning.
			vertexToPrune := selectVertexToPrune(degrees, outDegrees)
			if vertexToPrune == -1 {
				break // Exit loop if no vertex can be pruned.
			}

			// Mark the pruned vertex.
			fvs[vertexToPrune] = true
			delete(verticesToCheck, vertexToPrune)

			// Remove the vertex and its edges from the graph.
			cgCopy = removeVertexAndEdges(cgCopy, vertexToPrune)
			// Recalculate degrees after pruning.
			degrees, outDegrees = calculateDegrees(cgCopy, verticesToCheck)
			// Handle post-pruning scenarios.
			for i, degree := range degrees {
				if degree == 0 {
					delete(verticesToCheck, i)
					cgCopy = removeVertexAndEdges(cgCopy, i)
				}
			}
			for i, degree := range outDegrees {
				if degree == 0 {
					delete(verticesToCheck, i)
					cgCopy = removeVertexAndEdges(cgCopy, i)
				}
			}
		}
	}
	return fvs
}

// calculateDegrees calculates the in-degree and out-degree of vertices in a graph.
func calculateDegrees(cg [][]bool, verticesToCheck map[int]bool) (map[int]int, map[int]int) {
	degrees := make(map[int]int)
	outDegrees := make(map[int]int)
	for v := range verticesToCheck {
		for w, connected := range cg[v] {
			if connected {
				degrees[v]++
				if verticesToCheck[w] {
					outDegrees[v]++
				}
			}
		}
	}
	return degrees, outDegrees
}

// selectVertexToPrune selects the vertex with the maximum degree for pruning.
func selectVertexToPrune(degrees map[int]int, outDegrees map[int]int) int {
	var maxDegree = -1
	var selectedVertex = -1
	for v, degree := range degrees {
		if degree > maxDegree || (degree == maxDegree && outDegrees[v] < outDegrees[selectedVertex]) {
			maxDegree = degree
			selectedVertex = v
		}
	}
	return selectedVertex
}

// removeVertexAndEdges removes a vertex and its edges from the graph.
func removeVertexAndEdges(cg [][]bool, vertex int) [][]bool {
	for i := range cg {
		cg[i][vertex] = false
		cg[vertex][i] = false
	}
	return cg
}
