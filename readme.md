# Replication of the System Concurrency Experiment

The replication of the system concurrency experiment is divided into three parts: data acquisition (running Ethereum node programs for capture, public datasets), data processing (Python), and executing transactions based on the processed data (Go).

## Data Acquisition

1. Run an Ethereum client (such as Geth) to sync Ethereum data, and populate the synced database into `main.go` within `./export_evm_action/gendataset`. As detail：

   ```go
   var (
   	source_lvdb_path    = ""       // Address of the original lvdb database
   	source_ancient_path = ""       // Address of the original database's ancient folder
   	tmp_lvdb_path       = ""       // Location for a temporary leveldb, fill in arbitrarily
   	tmp_cfg_path        = ""       // Location for a temporary configuration file, fill in arbitrarily
   	startBlock_number   = 15000000 // Starting block number 15000000
   	endBlock_number     = 15150000 // Ending block number 15150000
   	output_CSV_path     = ""       // Output file location
   )
   ```

   run `main.go` to obtain historical state information."

2. Utilize public datasets available at [xblock.pro](https://xblock.pro/xblock-eth.html) to obtain transactions related to tokens from Ethereum's historical transactions. The datasets used in this experiment include "15000000to15249999_ERC20Transaction" and "15000000to15249999_ERC721Transaction".

## Data Processing

1. Run `1blockNumber_filter.py` to filter data from the public datasets, accelerating subsequent processing.
2. Execute `2append_executeTime.py` to merge the data captured by the Ethereum node program during the data acquisition phase. The output is a file containing all transactions' read/write sets and execution times.
3. Run `3Split_Total_Tx.py`, using the output file from step one and token data from the public datasets as inputs, to divide the transactions. The output files include all token and non-token transactions.
4. Execute `4vessel_process.py`, taking token data from public datasets as input, to transform the read/write sets of token contract transactions into vessel transaction read/write sets.
5. Run `5token_conflictRate.py` to obtain the transaction conflict rate and speedup bound presented in Figure 8.

## Transaction Execution

1. Copy the data processing stage-generated files `transaction.csv`, `transaction_token.csv`, `transaction_without_token.csv`, and `vessel.csv` to `./experiment`.

2. Run `main.go` in `./exp-init` to create a new level-db and populate it with data for simulating the execution of vessel transactions under real-world conditions.

3. Within `./experiment`, `main.go`'s `func main()` provides functions for executing contract transactions under three scenarios: all transactions, non-token transactions, and token transactions.

   - Serial execution of contract transactions:

     ```go
     serial(blocks, stateManager.stateMap, " ")
     ```

   - Parallel execution with occwsi:

     ```go
     occWsi(blocks, stateManager, "  ")
     ```

   - Parallel execution with deOCC:

     ```go
     deOCC(blocks, stateManager, "  ")
     ```

   Functions for executing vessel token transactions are also provided:

   - Serial execution of vessel transactions:

     ```go
     vessel_serial_execute()
     ```

   - Parallel execution of vessel transactions:

     ```go
     vessel_parallel_execute()
     ```

4. Retain all serial execution functions to obtain serial execution times in all scenarios. Comment out other execution functions.

5. Comment out all serial execution functions and retain parallel execution functions. Modify the `const` variable `thread` in `main.go` to control the number of goroutines during program execution, adjusting `thread` to obtain parallel execution times under different numbers of goroutines.

6. Compare parallel execution times with serial times to determine the parallel speedup ratios for different scenarios, specifically:

   - For token transaction execution analysis: The parallel speedup ratio for the account model is the ratio of serial to parallel execution times for contract token transactions. The parallel speedup ratio for the vessel model is the ratio of serial to parallel execution times for vessel token transactions.
   - For the analysis of all transaction executions: The parallel speedup ratio for the two smart contract parallelization schemes under comparison is the ratio of serial to parallel execution times for all transactions. The parallel speedup ratio for this scheme is calculated by adding the execution times for non-token transactions and vessel token transactions and dividing the serial execution time by the parallel time.

