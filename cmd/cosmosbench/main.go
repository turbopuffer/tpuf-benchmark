package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"

	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/spf13/cobra"
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "cosmosbench",
		Short: "Azure Cosmos DB benchmark CLI",
	}
	rootCmd.AddCommand(newSetupCommand(), newIngestCommand(), newIngestJSONCommand(), newQueryCommand())
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func cosmosClient() (*azcosmos.Client, error) {
	endpoint := os.Getenv("COSMOS_ENDPOINT")
	if endpoint == "" {
		return nil, errors.New("COSMOS_ENDPOINT environment variable must be provided")
	}
	key := os.Getenv("COSMOS_KEY")
	if key == "" {
		return nil, errors.New("COSMOS_KEY environment variable must be provided")
	}
	credential, err := azcosmos.NewKeyCredential(key)
	if err != nil {
		return nil, fmt.Errorf("creating Cosmos credential: %w", err)
	}
	client, err := azcosmos.NewClientWithKey(endpoint, credential, nil)
	if err != nil {
		return nil, fmt.Errorf("creating Cosmos client: %w", err)
	}
	return client, nil
}

func commandContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt)
}

func datasetCacheDir() string {
	if dir := os.Getenv("DATASET_CACHE_DIR"); dir != "" {
		return dir
	}
	return os.TempDir()
}
