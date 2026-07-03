#!/bin/bash

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
log_info "Checking prerequisites..."
if ! command -v docker &> /dev/null; then
    log_error "Docker is not installed"
    exit 1
fi

if ! command -v docker compose &> /dev/null; then
    log_error "docker compose is not available"
    exit 1
fi

if ! command -v curl &> /dev/null; then
    log_error "curl is not installed"
    exit 1
fi

# Discover the published host port for the RIPT service from Docker Compose.
# Fall back to .env or 8080 if the mapping cannot be resolved.
HTTP_PORT=""
if docker compose port ript 8080 >/dev/null 2>&1; then
    HTTP_PORT=$(docker compose port ript 8080 | awk -F: 'NF{print $NF; exit}')
fi
if [ -z "$HTTP_PORT" ] && [ -f "$SCRIPT_DIR/.env" ]; then
    _port=$(grep -E '^RIPT_HTTP_PORT=' "$SCRIPT_DIR/.env" | cut -d'=' -f2 | tr -d '[:space:]')
    if [ -n "$_port" ]; then
        HTTP_PORT="$_port"
    fi
fi
if [ -z "$HTTP_PORT" ]; then
    HTTP_PORT=8080
fi
TRACKER_BASE="http://localhost:${HTTP_PORT}"
log_info "Using RIPT port: $HTTP_PORT"

log_info "Starting Docker Compose stack..."
docker compose up -d --build

log_info "Waiting for Kafka to be ready..."
KAFKA_READY=false
for i in {1..30}; do
    if docker compose exec -T kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null; then
        log_info "Kafka is ready!"
        KAFKA_READY=true
        break
    fi
    sleep 2
done

if [ "$KAFKA_READY" = "false" ]; then
    log_error "Kafka did not become ready in time"
    exit 1
fi

log_info "Waiting for RIPT API to be ready..."
TRACKER_READY=false
for i in {1..30}; do
    if curl -s "${TRACKER_BASE}/api/health" > /dev/null 2>&1; then
        log_info "RIPT API is ready!"
        TRACKER_READY=true
        break
    fi
    sleep 2
done

if [ "$TRACKER_READY" = "false" ]; then
    log_error "RIPT API did not become ready in time"
    exit 1
fi

log_info "Creating test topics..."
docker compose exec -T kafka kafka-topics --create --topic test-active --bootstrap-server kafka:9092 --partitions 2 --replication-factor 1 --if-not-exists
docker compose exec -T kafka kafka-topics --create --topic test-inactive --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1 --if-not-exists

log_info "Producing messages to test-active topic..."
echo "test-message-1" | docker compose exec -T kafka kafka-console-producer --topic test-active --broker-list kafka:9092
echo "test-message-2" | docker compose exec -T kafka kafka-console-producer --topic test-active --broker-list kafka:9092

log_info "Waiting for first scan cycle (this may take up to 1 minute)..."
sleep 60

log_info "Running API tests..."

# Test 1: Health check
log_info "Test 1: Health check..."
HEALTH=$(curl -s "${TRACKER_BASE}/api/health" | grep -q '"status":"healthy"' && echo "PASS" || echo "FAIL")
if [ "$HEALTH" = "PASS" ]; then
    log_info "✓ Health check passed"
else
    log_error "✗ Health check failed"
    exit 1
fi

# Test 2: Get all topics
log_info "Test 2: Get all topics..."
TOPICS=$(curl -s "${TRACKER_BASE}/api/topics")
if echo "$TOPICS" | grep -q '"count"'; then
    TOPIC_COUNT=$(echo "$TOPICS" | grep -o '"count":[0-9]*' | grep -o '[0-9]*')
    log_info "✓ Found $TOPIC_COUNT topics"
    if [ "$TOPIC_COUNT" -ge 2 ]; then
        log_info "✓ Expected test topics found"
    else
        log_warn "⚠ Expected at least 2 test topics"
    fi
else
    log_error "✗ Failed to get topics"
    exit 1
fi

# Test 3: Get specific topic
log_info "Test 3: Get specific topic..."
TOPIC_DETAIL=$(curl -s "${TRACKER_BASE}/api/topics/test-active")
if echo "$TOPIC_DETAIL" | grep -q '"name":"test-active"'; then
    log_info "✓ Retrieved test-active topic details"
else
    log_error "✗ Failed to get test-active topic"
    exit 1
fi

# Test 3b: Ignore/unignore workflow persists and affects filtering
log_info "Test 3b: Ignore/unignore workflow..."
IGNORE_RESP=$(curl -s -X PUT -H 'Content-Type: application/json' -d '{"ignored":true}' "${TRACKER_BASE}/api/topics/test-inactive/ignored")
if echo "$IGNORE_RESP" | grep -q '"ignored":true'; then
    log_info "✓ Marked test-inactive as ignored"
else
    log_error "✗ Failed to mark test-inactive as ignored"
    exit 1
fi

DEFAULT_FILTER=$(curl -s "${TRACKER_BASE}/api/topics?search=test-inactive")
if echo "$DEFAULT_FILTER" | grep -q '"count":0'; then
    log_info "✓ Ignored topic hidden from default topic list"
else
    log_error "✗ Ignored topic should be hidden from default topic list"
    exit 1
fi

ALL_FILTER=$(curl -s "${TRACKER_BASE}/api/topics?ignored=all&search=test-inactive")
if echo "$ALL_FILTER" | grep -q '"count":1'; then
    log_info "✓ Ignored topic visible with ignored=all"
else
    log_error "✗ Ignored topic should be visible with ignored=all"
    exit 1
fi

log_info "Restarting RIPT to verify ignored state is persisted..."
docker compose restart ript

log_info "Waiting for RIPT API to be ready after restart..."
TRACKER_READY=false
for i in {1..30}; do
    if curl -s "${TRACKER_BASE}/api/health" > /dev/null 2>&1; then
        log_info "RIPT API is ready after restart!"
        TRACKER_READY=true
        break
    fi
    sleep 2
done

if [ "$TRACKER_READY" = "false" ]; then
    log_error "RIPT API did not become ready after restart"
    exit 1
fi

PERSISTED_DETAIL=$(curl -s "${TRACKER_BASE}/api/topics/test-inactive")
if echo "$PERSISTED_DETAIL" | grep -q '"ignored":true'; then
    log_info "✓ Ignored state persisted across restart"
else
    log_error "✗ Ignored state did not persist across restart"
    exit 1
fi

UNIGNORE_RESP=$(curl -s -X PUT -H 'Content-Type: application/json' -d '{"ignored":false}' "${TRACKER_BASE}/api/topics/test-inactive/ignored")
if echo "$UNIGNORE_RESP" | grep -q '"ignored":false'; then
    log_info "✓ Cleared ignored state for test-inactive"
else
    log_error "✗ Failed to clear ignored state for test-inactive"
    exit 1
fi

# Test 4: Get statistics
log_info "Test 4: Get cluster statistics..."
STATS=$(curl -s "${TRACKER_BASE}/api/stats")
if echo "$STATS" | grep -q '"total_topics"'; then
    log_info "✓ Retrieved cluster statistics"
else
    log_error "✗ Failed to get statistics"
    exit 1
fi

# Test 5: Get unused topics
log_info "Test 5: Get unused topics..."
UNUSED=$(curl -s "${TRACKER_BASE}/api/unused?unused_days=30")
if echo "$UNUSED" | grep -q '"count"'; then
    log_info "✓ Retrieved unused topics"
else
    log_error "✗ Failed to get unused topics"
    exit 1
fi

# Test 6: Dashboard loads
log_info "Test 6: Dashboard loads..."
DASHBOARD=$(curl -s "${TRACKER_BASE}/")
if echo "$DASHBOARD" | grep -q "RIPT"; then
    log_info "✓ Dashboard loads successfully"
else
    log_error "✗ Dashboard failed to load"
    exit 1
fi

log_info "All tests passed! ✓"
log_info "Dashboard is available at ${TRACKER_BASE}"

log_info "To stop the stack, run: docker compose down"
log_info "To view logs, run: docker compose logs -f tracker"
