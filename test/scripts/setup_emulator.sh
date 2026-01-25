#!/bin/bash
# Start Firebase Emulator with test data for FireDuckExt integration tests

set -e

# Check if firebase-tools is installed
if ! command -v firebase &> /dev/null; then
    echo "Firebase CLI not found. Installing..."
    npm install -g firebase-tools
fi

# Set environment variable for extension to use emulator
export FIRESTORE_EMULATOR_HOST="127.0.0.1:8080"

# Start emulator in background
echo "Starting Firestore emulator..."
firebase emulators:start --only firestore --project test-project &
EMULATOR_PID=$!

# Wait for emulator to be ready
echo "Waiting for emulator to start..."
sleep 10

# Check if emulator is running
if ! curl -s "http://127.0.0.1:8080" > /dev/null 2>&1; then
    echo "Emulator failed to start"
    exit 1
fi

echo "Emulator is running. Seeding test data..."

# Seed test data via REST API
# Users collection
curl -s -X POST "http://127.0.0.1:8080/v1/projects/test-project/databases/(default)/documents/users?documentId=user1" \
  -H "Content-Type: application/json" \
  -d '{"fields": {"name": {"stringValue": "Alice"}, "age": {"integerValue": "30"}, "status": {"stringValue": "active"}}}'

curl -s -X POST "http://127.0.0.1:8080/v1/projects/test-project/databases/(default)/documents/users?documentId=user2" \
  -H "Content-Type: application/json" \
  -d '{"fields": {"name": {"stringValue": "Bob"}, "age": {"integerValue": "25"}, "status": {"stringValue": "pending"}}}'

curl -s -X POST "http://127.0.0.1:8080/v1/projects/test-project/databases/(default)/documents/users?documentId=user3" \
  -H "Content-Type: application/json" \
  -d '{"fields": {"name": {"stringValue": "Charlie"}, "age": {"integerValue": "35"}, "status": {"stringValue": "inactive"}}}'

# Products collection
curl -s -X POST "http://127.0.0.1:8080/v1/projects/test-project/databases/(default)/documents/products?documentId=prod1" \
  -H "Content-Type: application/json" \
  -d '{"fields": {"name": {"stringValue": "Widget"}, "price": {"doubleValue": 9.99}, "inStock": {"booleanValue": true}}}'

curl -s -X POST "http://127.0.0.1:8080/v1/projects/test-project/databases/(default)/documents/products?documentId=prod2" \
  -H "Content-Type: application/json" \
  -d '{"fields": {"name": {"stringValue": "Gadget"}, "price": {"doubleValue": 19.99}, "inStock": {"booleanValue": false}}}'

# Types test collection (all Firestore types)
curl -s -X POST "http://127.0.0.1:8080/v1/projects/test-project/databases/(default)/documents/types_test?documentId=all_types" \
  -H "Content-Type: application/json" \
  -d '{
    "fields": {
      "stringField": {"stringValue": "hello"},
      "intField": {"integerValue": "42"},
      "doubleField": {"doubleValue": 3.14},
      "boolField": {"booleanValue": true},
      "timestampField": {"timestampValue": "2024-01-15T10:30:00Z"},
      "arrayField": {"arrayValue": {"values": [{"integerValue": "1"}, {"integerValue": "2"}, {"integerValue": "3"}]}},
      "mapField": {"mapValue": {"fields": {"nested": {"stringValue": "value"}}}},
      "nullField": {"nullValue": null}
    }
  }'

echo ""
echo "Test data seeded successfully!"
echo "Emulator PID: $EMULATOR_PID"
echo ""
echo "To run tests:"
echo "  export FIRESTORE_EMULATOR_HOST=\"127.0.0.1:8080\""
echo "  cd build/release && ctest --output-on-failure"
echo ""
echo "Press Ctrl+C to stop the emulator"

# Wait for emulator process
wait $EMULATOR_PID
