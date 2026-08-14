package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/spf13/cobra"
)

type setupConfig struct {
	database string
	maxRU    int32
}

func newSetupCommand() *cobra.Command {
	cfg := setupConfig{}
	cmd := &cobra.Command{
		Use:   "setup",
		Short: "Create the benchmark database and containers",
		RunE: func(cmd *cobra.Command, _ []string) error {
			ctx, cancel := commandContext()
			defer cancel()
			return runSetup(ctx, cmd, cfg)
		},
	}
	cmd.Flags().StringVar(&cfg.database, "database", "tpufbench", "database name")
	cmd.Flags().Int32Var(&cfg.maxRU, "max-ru", 10000, "per-container autoscale maximum RU/s")
	return cmd
}

func runSetup(ctx context.Context, cmd *cobra.Command, cfg setupConfig) error {
	if cfg.maxRU <= 0 {
		return errors.New("max-ru must be positive")
	}
	client, err := cosmosClient()
	if err != nil {
		return err
	}
	_, err = client.CreateDatabase(ctx, azcosmos.DatabaseProperties{ID: cfg.database}, nil)
	if isStatus(err, http.StatusConflict) {
		fmt.Fprintf(cmd.OutOrStdout(), "database %s already exists\n", cfg.database)
	} else if err != nil {
		return fmt.Errorf("creating database %s: %w", cfg.database, err)
	} else {
		fmt.Fprintf(cmd.OutOrStdout(), "created database %s\n", cfg.database)
	}

	database, err := client.NewDatabase(cfg.database)
	if err != nil {
		return fmt.Errorf("opening database %s: %w", cfg.database, err)
	}
	containers := []azcosmos.ContainerProperties{vectorContainerProperties(), textContainerProperties()}
	for _, properties := range containers {
		throughput := azcosmos.NewAutoscaleThroughputProperties(cfg.maxRU)
		_, err := database.CreateContainer(ctx, properties, &azcosmos.CreateContainerOptions{ThroughputProperties: &throughput})
		if isStatus(err, http.StatusConflict) {
			fmt.Fprintf(cmd.OutOrStdout(), "container %s already exists\n", properties.ID)
		} else if err != nil {
			return fmt.Errorf("creating container %s: %w", properties.ID, err)
		} else {
			fmt.Fprintf(cmd.OutOrStdout(), "created container %s\n", properties.ID)
		}
	}
	return nil
}

func vectorContainerProperties() azcosmos.ContainerProperties {
	return azcosmos.ContainerProperties{
		ID:                     "vectors",
		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{Paths: []string{"/pk"}},
		VectorEmbeddingPolicy: &azcosmos.VectorEmbeddingPolicy{VectorEmbeddings: []azcosmos.VectorEmbedding{{
			Path: "/vector", DataType: azcosmos.VectorDataTypeFloat16, Dimensions: 1024, DistanceFunction: azcosmos.VectorDistanceFunctionCosine,
		}}},
		IndexingPolicy: &azcosmos.IndexingPolicy{
			Automatic: true, IndexingMode: azcosmos.IndexingModeConsistent,
			IncludedPaths: []azcosmos.IncludedPath{}, ExcludedPaths: []azcosmos.ExcludedPath{{Path: "/*"}},
			VectorIndexes: []azcosmos.VectorIndex{{Path: "/vector", Type: azcosmos.VectorIndexTypeDiskANN}},
		},
	}
}

func textContainerProperties() azcosmos.ContainerProperties {
	return azcosmos.ContainerProperties{
		ID:                     "text",
		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{Paths: []string{"/pk"}},
		FullTextPolicy: &azcosmos.FullTextPolicy{
			DefaultLanguage: "en-US", FullTextPaths: []azcosmos.FullTextPath{{Path: "/text", Language: "en-US"}},
		},
		IndexingPolicy: &azcosmos.IndexingPolicy{
			Automatic: true, IndexingMode: azcosmos.IndexingModeConsistent,
			IncludedPaths: []azcosmos.IncludedPath{}, ExcludedPaths: []azcosmos.ExcludedPath{{Path: "/*"}},
			FullTextIndexes: []azcosmos.FullTextIndex{{Path: "/text"}},
		},
	}
}

func isStatus(err error, status int) bool {
	var responseErr *azcore.ResponseError
	return errors.As(err, &responseErr) && responseErr.StatusCode == status
}
