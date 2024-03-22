import numpy as np
import pandas as pd
from tqdm import tqdm

# Function to calculate weighted average
def calculate_weighted_average(group):
    group['graph1_weighted'] = group['graph1_ratio'] * group['graph1_nodes']
    group['graph2_weighted'] = group['graph2_ratio'] * group['graph2_nodes']
    total_nodes_g1 = group['graph1_nodes'].sum()
    total_nodes_g2 = group['graph2_nodes'].sum()
    weighted_avg_g1 = group['graph1_weighted'].sum() / total_nodes_g1 if total_nodes_g1 else np.nan
    weighted_avg_g2 = group['graph2_weighted'].sum() / total_nodes_g2 if total_nodes_g2 else np.nan
    group['graph1_single_weighted'] = group['graph1_single_ratio'] * group['graph1_nodes']
    group['graph2_single_weighted'] = group['graph2_single_ratio'] * group['graph2_nodes']

    weighted_avg_g1_single = group['graph1_single_weighted'].sum() / total_nodes_g1 if total_nodes_g1 else np.nan
    weighted_avg_g2_single = group['graph2_single_weighted'].sum() / total_nodes_g2 if total_nodes_g2 else np.nan

    return pd.Series([weighted_avg_g1, weighted_avg_g2, weighted_avg_g1_single, weighted_avg_g2_single, total_nodes_g1,
                      total_nodes_g2],
                     index=['graph1_weighted_avg', 'graph2_weighted_avg', 'graph1_single_weighted_avg',
                            'graph2_single_weighted_avg', 'total_nodes_g1', 'total_nodes_g2'])

class Graph:
    def __init__(self):
        self.graph = {}
        self.parent = {}

    def add_node(self, node):
        if node not in self.graph:
            self.graph[node] = []
            self.parent[node] = node

    def add_edge(self, node1, node2):
        self.add_node(node1)
        self.add_node(node2)
        self.graph[node1].append(node2)

    # Calculate the maximum depth starting from a specific node
    def dfs_max_depth(self, node, visited=None, depth=0):
        if visited is None:
            visited = set()
        visited.add(node)
        max_depth = depth
        for neighbor in self.graph.get(node, []):
            if neighbor not in visited:
                max_depth = max(max_depth, self.dfs_max_depth(neighbor, visited, depth + 1))
        return max_depth

    # Calculate the maximum depth of all nodes in the graph
    def max_depth(self):
        max_depth = 0
        visited = set()
        for node in self.graph.keys():
            if node not in visited:
                max_depth = max(max_depth, self.dfs_max_depth(node, visited))
        return max_depth

    def count_edges_nodes(self):
        # Calculate the number of nodes with edges
        edges_nodes = {node for node, neighbors in self.graph.items() if neighbors}
        return len(edges_nodes)


def process_block(block_data):
    graph1 = Graph()
    graph2 = Graph()
    for i in range(len(block_data)):
        graph1.add_node(block_data[i]['transactionHash'])
        graph2.add_node(block_data[i]['transactionHash'])
        for j in range(i + 1, len(block_data)):
            if block_data[i]['transactionHash'] == block_data[j]['transactionHash']:
                continue
            if (block_data[i]['tokenAddress'] == block_data[j]['tokenAddress']) and (
                    block_data[j]['from'] == block_data[i]['from'] or block_data[j]['from'] == block_data[i]['to'] or
                    block_data[j]['to'] == block_data[i]['to'] or block_data[j]['to'] == block_data[i]['from']):
                graph1.add_edge(block_data[i]['transactionHash'], block_data[j]['transactionHash'])
                if block_data[j]['from'] == block_data[i]['to'] or block_data[j]['to'] == block_data[i]['from']:
                    graph2.add_edge(block_data[i]['transactionHash'], block_data[j]['transactionHash'])

    # Calculate the ratio of nodes with edges
    graph1_edges_nodes_ratio = graph1.count_edges_nodes() / len(graph1.graph) if len(graph1.graph) else 0
    graph2_edges_nodes_ratio = graph2.count_edges_nodes() / len(graph2.graph) if len(graph2.graph) else 0
    return len(graph1.graph), graph1.max_depth(), len(graph2.graph), graph2.max_depth(), graph1_edges_nodes_ratio, graph2_edges_nodes_ratio


def main():
    filenames = ['15000001to15010000_ERC20Transaction.csv', '15000001to15010000_ERC721Transaction.csv']
    combined_df = pd.DataFrame()
    for filename in filenames:
        df = pd.read_csv(filename)
        combined_df = pd.concat([combined_df, df], ignore_index=True)

    # Ensure 'blockNumber' column exists for subsequent grouping
    combined_df_grouped = combined_df.groupby('blockNumber')
    results = []
    for block_number, block_data in tqdm(combined_df_grouped, total=combined_df_grouped.ngroups):
        block_data_dict = block_data.to_dict('records')
        result = process_block(block_data_dict)
        results.append([block_number] + list(result))

    result_df = pd.DataFrame(results, columns=['blockNumber', 'graph1_nodes', 'graph1_max_depth', 'graph2_nodes',
                                               'graph2_max_depth', 'graph1_single_ratio', 'graph2_single_ratio'])
    result_df['graph1_ratio'] = result_df['graph1_max_depth'] / result_df['graph1_nodes']
    result_df['graph2_ratio'] = result_df['graph2_max_depth'] / result_df['graph2_nodes']
    result_df.to_csv('token_conflict_combined.csv', index=False)

    # Include new statistics in weighted average calculation afterwards
    grouped = result_df.groupby(result_df.index // 200)
    weighted_avgs = grouped.apply(calculate_weighted_average).reset_index()
    weighted_avgs.to_csv('weighted_token_conflict_combined.csv', index=False)

if __name__ == '__main__':
    main()
