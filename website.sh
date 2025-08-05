#!/bin/bash

# Website benchmark script
# Reproduces the benchmarks shown on the turbopuffer website

set -e

# Check for API key
if [ -z "$TURBOPUFFER_API_KEY" ]; then
    echo "⚠️  TURBOPUFFER_API_KEY not set"
    exit 1
fi

ENDPOINT="${TURBOPUFFER_ENDPOINT:-https://api.turbopuffer.com}"

# Function to run benchmark with common parameters
run_benchmark() {
    local name="$1"
    local doc_template="$2"
    local query_template="$3"
    local upsert_template="$4"
    local namespace_size="$5"
    local benchmark_duration="$6"
    local cache_flags="$7"

    # Remove prior results directory to avoid errors
    if [ -d "website-$name-results" ]; then
        echo "🗑️  Removing prior results directory: website-$name-results"
        rm -rf "website-$name-results"
    fi

    echo "🚀 Running $name benchmark..."
    echo "   - Documents: $namespace_size"
    echo "   - Duration: $benchmark_duration"
    echo "   - Templates: $doc_template, $query_template, $upsert_template"
    echo ""

    go run . \
        --api-key="$TURBOPUFFER_API_KEY" \
        --endpoint="$ENDPOINT" \
        --document-template="$doc_template" \
        --query-template="$query_template" \
        --upsert-template="$upsert_template" \
        --namespace-prefix="website-$name-$(date +"%Y-%m-%d")" \
        --namespace-count=1 \
        --namespace-combined-size="$namespace_size" \
        --benchmark-duration="$benchmark_duration" \
        --queries-per-sec=32 \
        --upserts-per-sec=0 \
        $cache_flags \
        --output-dir="website-$name-results"
}

# Parse command line arguments
case "${1:-help}" in
vector-warm)
    echo "📊 Website Benchmark: Vector Performance (Warm Namespace)"
    echo "   Workload: 768 dimensions, 1M docs, ~3GB"
    run_benchmark "vector-warm" \
        "templates/document_default.json.tmpl" \
        "templates/query_default.json.tmpl" \
        "templates/upsert_default.json.tmpl" \
        1000000 \
        5m \
        "--warm-cache"
    ;;

vector-cold)
    echo "📊 Website Benchmark: Vector Performance (Cold Namespace)"
    echo "   Workload: 768 dimensions, 1M docs, ~3GB"
    run_benchmark "vector-cold" \
        "templates/document_default.json.tmpl" \
        "templates/query_cold.json.tmpl" \
        "templates/upsert_default.json.tmpl" \
        1000000 \
        5m \
        "--purge-cache"
    ;;

fulltext-warm)
    echo "📊 Website Benchmark: Full-Text Performance (Warm Namespace)"
    echo "   Workload: BM25, 1M docs, ~300MB"
    run_benchmark "fulltext-warm" \
        "templates/document_full_text.json.tmpl" \
        "templates/query_full_text.json.tmpl" \
        "templates/upsert_full_text.json.tmpl" \
        1000000 \
        5m \
        "--warm-cache"
    ;;

fulltext-cold)
    echo "📊 Website Benchmark: Full-Text Performance (Cold Namespace)"
    echo "   Workload: BM25, 1M docs, ~300MB"
    run_benchmark "fulltext-cold" \
        "templates/document_full_text.json.tmpl" \
        "templates/query_full_text_cold.json.tmpl" \
        "templates/upsert_full_text.json.tmpl" \
        1000000 \
        5m \
        "--purge-cache"
    ;;

single-doc-upsert)
    echo "📊 Website Benchmark: Single Document Upsert Latency"
    echo "   Workload: Individual document upserts (batch size=1)"
    echo "   Testing latency for single document operations"

    # Remove prior results directory to avoid errors
    if [ -d "website-single-doc-upsert-results" ]; then
        echo "🗑️  Removing prior results directory: website-single-doc-upsert-results"
        rm -rf "website-single-doc-upsert-results"
    fi

    # For single doc upserts, we'll use a smaller namespace and focus on upserts
    go run . \
        --api-key="$TURBOPUFFER_API_KEY" \
        --endpoint="$ENDPOINT" \
        --document-template="templates/document_default.json.tmpl" \
        --query-template="templates/query_default.json.tmpl" \
        --upsert-template="templates/upsert_default.json.tmpl" \
        --namespace-prefix="website-single-doc-$(date +%s)" \
        --namespace-count=1 \
        --namespace-combined-size=1000 \
        --benchmark-duration="10m" \
        --queries-per-sec=0 \
        --upserts-per-sec=10 \
        --upsert-batch-size=1 \
        --prompt-to-clear=false \
        --output-dir="website-single-doc-upsert-results"
    ;;

help | *)
    echo "Website Benchmark Script"
    echo "========================"
    echo ""
    echo "Reproduces the benchmarks shown on the turbopuffer website:"
    echo "- Vector search: 768 dimensions, 1M docs, ~3GB"
    echo "- Full-text search: BM25, 1M docs, ~300MB"
    echo "- Both warm and cold cache scenarios"
    echo "- 3 QPS with topk=10 approach"
    echo ""
    echo "Usage: $0 <benchmark-type>"
    echo ""
    echo "Available benchmark types:"
    echo "  vector-warm       - Vector search with warm cache"
    echo "  vector-cold       - Vector search with cold cache"
    echo "  fulltext-warm     - Full-text search with warm cache"
    echo "  fulltext-cold     - Full-text search with cold cache"
    echo "  single-doc-upsert - Single document upsert latency test"
    echo ""
    echo "Environment variables:"
    echo "  TURBOPUFFER_API_KEY - Required API key"
    echo "  TURBOPUFFER_ENDPOINT - API endpoint (default: https://api.turbopuffer.com)"
    echo ""
    echo "Examples:"
    echo "  $0 vector-warm"
    echo "  $0 fulltext-cold"
    echo ""
    exit 0
    ;;
esac

# Special handling for single-doc-upsert output directory
if [ "$1" = "single-doc-upsert" ]; then
    results_dir="website-single-doc-upsert-results"
else
    results_dir="website-$1-results"
fi

echo ""
echo "✅ $1 benchmark complete!"
echo "📈 Results saved to: $results_dir/"

# Extract and display key metrics from the report
if [ -f "$results_dir/report.json" ]; then
    echo ""
    echo "📊 Benchmark Results Summary:"
    echo "============================="

    # Check if jq is available
    if command -v jq &>/dev/null; then
        # Query latencies
        for temp in cold warm hot; do
            if jq -e ".${temp}_queries" "$results_dir/report.json" >/dev/null 2>&1; then
                echo ""
                echo "🔍 ${temp^} Queries:"
                echo "  Count: $(jq -r ".${temp}_queries.count" "$results_dir/report.json")"
                echo "  Throughput: $(jq -r ".${temp}_queries.throughput" "$results_dir/report.json" | xargs printf "%.2f") req/s"
                echo "  Server Latencies: $(jq -r ".${temp}_queries.latencies" "$results_dir/report.json")"
            fi
        done

        # Upsert latencies (if present)
        if jq -e ".upserts" "$results_dir/report.json" >/dev/null 2>&1; then
            echo ""
            echo "📝 Upserts:"
            echo "  Requests: $(jq -r ".upserts.num_requests" "$results_dir/report.json")"
            echo "  Documents: $(jq -r ".upserts.num_documents" "$results_dir/report.json")"
            echo "  Total bytes: $(jq -r ".upserts.total_bytes" "$results_dir/report.json" | numfmt --to=iec-i --suffix=B 2>/dev/null || jq -r ".upserts.total_bytes" "$results_dir/report.json")"
            echo "  Throughput: $(jq -r ".upserts.throughput" "$results_dir/report.json" | xargs printf "%.2f") req/s"
            # Calculate MB/s throughput
            total_bytes=$(jq -r ".upserts.total_bytes" "$results_dir/report.json")
            num_requests=$(jq -r ".upserts.num_requests" "$results_dir/report.json")
            throughput=$(jq -r ".upserts.throughput" "$results_dir/report.json")
            if [ "$throughput" != "0" ] && [ "$throughput" != "null" ] && [ "$num_requests" != "0" ]; then
                bytes_per_request=$(echo "scale=2; $total_bytes / $num_requests" | bc)
                mb_per_sec=$(echo "scale=2; $bytes_per_request * $throughput / 1048576" | bc)
                echo "  Throughput: ${mb_per_sec} MB/s"
            fi
            echo "  Client Latencies: $(jq -r ".upserts.latencies" "$results_dir/report.json")"
        fi
    else
        echo ""
        echo "⚠️  Install jq to see detailed results summary"
        echo "   Full results in: $results_dir/report.json"
    fi
    echo ""
fi
