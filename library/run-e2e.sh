#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_POM="$SCRIPT_DIR/../pom.xml"

cleanup() {
    echo "Stopping services..."
    kill "$MEMBER_PID" "$TELEPORT_PID" "$CHECKOUT_PID" 2>/dev/null || true
}
trap cleanup EXIT

wait_for() {
    local url=$1
    echo "Waiting for $url ..."
    for i in $(seq 1 30); do
        if curl -sf "$url" > /dev/null 2>&1; then
            echo "  ready"
            return 0
        fi
        sleep 2
    done
    echo "  ERROR: timed out waiting for $url"
    exit 1
}

echo "=== Building library services ==="
mvn install -pl library/member,library/teleport,library/checkout -am \
    -DskipTests -f "$ROOT_POM" -q

echo "=== Starting services ==="
java -jar "$SCRIPT_DIR/member/target/member-1.0.jar"   > /tmp/member.log   2>&1 & MEMBER_PID=$!
java -jar "$SCRIPT_DIR/teleport/target/teleport-1.0.jar" > /tmp/teleport.log 2>&1 & TELEPORT_PID=$!
java -jar "$SCRIPT_DIR/checkout/target/checkout-1.0.jar" > /tmp/checkout.log 2>&1 & CHECKOUT_PID=$!

wait_for "http://localhost:8082/actuator/health"
wait_for "http://localhost:8083/actuator/health"
wait_for "http://localhost:8085/actuator/health"

echo "=== Running E2E tests ==="
mvn test -pl library/e2e -f "$ROOT_POM"

echo "=== All tests passed ==="
