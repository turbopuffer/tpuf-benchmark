package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"
)

// This endpoint is common with most cloud providers, aka should work on GCP, AWS, Azure, etc.
// We use this to determine if we are running on a cloud VM, and log a warning if we aren't.
const metadataUrl = "169.254.169.254"

func runningOnCloudVM(ctx context.Context) (bool, error) {
	timedCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(timedCtx, "GET", "http://"+metadataUrl, nil)
	if err != nil {
		return false, fmt.Errorf("new request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return false, nil
		}
		return false, fmt.Errorf("do request: %w", err)
	}

	return resp.StatusCode == http.StatusOK, nil
}

func logWarningIfNotOnCloudVM(ctx context.Context, logger *slog.Logger) error {
	onCloudVM, err := runningOnCloudVM(ctx)
	if err != nil {
		return fmt.Errorf("failed to check if running on cloud vm: %w", err)
	}

	if !onCloudVM {
		logger.Warn(
			"script is likely not running on a cloud VM. this benchmark is designed to be run from a VM in the same region as the turbopuffer deployment, results will likely be inaccurate",
		)
	}

	return nil
}
