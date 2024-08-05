package main

import (
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"

	tendermint "github.com/cosmos/cosmos-sdk/client/grpc/cmtservice"
	txtypes "github.com/cosmos/cosmos-sdk/types/tx"
	"google.golang.org/grpc"
)

var (
	grpcURL        string
	startBlock     int64
	endBlock       int64
	compareAddress string
	outputFilename string
	maxConcurrent  int
	grpcConn       *grpc.ClientConn
	blockService   tendermint.ServiceClient
	txService      txtypes.ServiceClient
	mutex          sync.Mutex
)

func init() {
	flag.StringVar(&grpcURL, "grpc_url", "", "The gRPC endpoint URL")
	flag.Int64Var(&startBlock, "start_block", 0, "The starting block height")
	flag.Int64Var(&endBlock, "end_block", 0, "The ending block height")
	flag.StringVar(&compareAddress, "compare_address", "", "The address to compare in transactions")
	flag.StringVar(&outputFilename, "output_filename", "output.csv", "The output CSV filename")
	flag.IntVar(&maxConcurrent, "max_concurrent", 10, "Max number of concurrent goroutines")
}

func main() {
	flag.Parse()

	var err error
	grpcConn, err = grpc.Dial(grpcURL, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to gRPC server: %v", err)
	}
	defer grpcConn.Close()

	blockService = tendermint.NewServiceClient(grpcConn)
	txService = txtypes.NewServiceClient(grpcConn)

	file, err := os.OpenFile(outputFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Fatalf("Failed to open output file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()
	writer.Write([]string{"Date Time", "Block", "TxHash", "Fee"})

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, maxConcurrent)

	for blockHeight := endBlock; blockHeight >= startBlock; blockHeight-- {
		wg.Add(1)
		semaphore <- struct{}{}

		go func(height int64) {
			defer wg.Done()
			defer func() { <-semaphore }()

			processBlock(height, writer)
		}(blockHeight)
	}

	wg.Wait()
}

func processBlock(blockHeight int64, writer *csv.Writer) {
	blockResp, err := blockService.GetBlockByHeight(context.Background(), &tendermint.GetBlockByHeightRequest{Height: blockHeight})
	if err != nil {
		return
	}

	block := blockResp.Block
	timestamp := block.Header.Time

	for _, txRaw := range block.Data.Txs {
		txHash := hex.EncodeToString(hashFunc(txRaw))

		txResp, err := txService.GetTx(context.Background(), &txtypes.GetTxRequest{Hash: txHash})
		if err != nil {
			continue
		}

		txDetails := txResp.Tx.String()
		if strings.Contains(txDetails, compareAddress) {
			var feeAmount string
			if len(txResp.Tx.AuthInfo.Fee.Amount) > 0 {
				amountInt64 := txResp.Tx.AuthInfo.Fee.Amount[0].Amount.Int64()
				feeAmount = fmt.Sprintf("%f", float64(amountInt64)/1000000)
			} else {
				feeAmount = "0"
			}
			record := []string{timestamp.String(), fmt.Sprintf("%d", blockHeight), txHash, feeAmount}

			mutex.Lock()
			writer.Write(record)
			writer.Flush()
			mutex.Unlock()
		}
	}
}

func hashFunc(data []byte) []byte {
	h := sha256.New()
	h.Write(data)
	return h.Sum(nil)
}
