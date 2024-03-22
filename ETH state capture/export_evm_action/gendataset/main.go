package main

import (
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/gendataset/fff"
)

var (
	source_lvdb_path    = ""       // Address of the original lvdb database
	source_ancient_path = ""       // Address of the original database's ancient folder
	tmp_lvdb_path       = ""       // Location for a temporary leveldb, fill in arbitrarily
	tmp_cfg_path        = ""       // Location for a temporary configuration file, fill in arbitrarily
	startBlock_number   = 15000000 // Starting block number 15000000
	endBlock_number     = 15150000 // Ending block number 15150000
	output_CSV_path     = ""       // Output file location
)

func main() {
	dbSSD, _ := rawdb.NewLevelDBDatabaseWithFreezer(source_lvdb_path, 1024, 256, source_ancient_path, "", true)

	defer dbSSD.Close()
	read_db := dbSSD

	writeTmp_db_lvdb1, _ := rawdb.NewLevelDBDatabase(tmp_lvdb_path, 1024, 256, "", false)
	var (
		startC = uint64(startBlock_number)
		endC   = uint64(endBlock_number)
	)

	go fff.Gendata(startC, endC, &writeTmp_db_lvdb1, &read_db, output_CSV_path, tmp_cfg_path)
	for {

	}
}
