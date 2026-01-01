#!/bin/bash

# quickset http api usage examples

BASE_URL="http://localhost:8080"

echo "=== quickset api examples ==="
echo

# create table
echo "1. creating table 'users'..."
curl -s -X POST "$BASE_URL/table/create" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "users",
    "columns": [
      {"name": "id", "type": "int"},
      {"name": "name", "type": "string"},
      {"name": "email", "type": "string"},
      {"name": "age", "type": "int"}
    ],
    "capacity": 1000000
  }'
echo
echo

# insert data
echo "2. inserting rows..."
curl -s -X POST "$BASE_URL/insert" \
  -H "Content-Type: application/json" \
  -d '{
    "table": "users",
    "rows": [
      [1, "alice", "alice@example.com", 30],
      [2, "bob", "bob@example.com", 25],
      [3, "charlie", "charlie@example.com", 35],
      [4, "alice smith", "asmith@example.com", 28]
    ]
  }'
echo
echo

# exact search
echo "3. exact search (find name = 'alice')..."
curl -s -X POST "$BASE_URL/search" \
  -H "Content-Type: application/json" \
  -d '{
    "table": "users",
    "column": "name",
    "type": "exact",
    "value": "alice"
  }'
echo
echo

# prefix search
echo "4. prefix search (find names starting with 'al')..."
curl -s -X POST "$BASE_URL/search" \
  -H "Content-Type: application/json" \
  -d '{
    "table": "users",
    "column": "name",
    "type": "prefix",
    "prefix": "al"
  }'
echo
echo

# fulltext search
echo "5. fulltext search (find 'alice' in name)..."
curl -s -X POST "$BASE_URL/search" \
  -H "Content-Type: application/json" \
  -d '{
    "table": "users",
    "column": "name",
    "type": "fulltext",
    "query": "alice"
  }'
echo
echo

# range search
echo "6. range search (find age between 25-32)..."
curl -s -X POST "$BASE_URL/search" \
  -H "Content-Type: application/json" \
  -d '{
    "table": "users",
    "column": "age",
    "type": "range",
    "min": 25,
    "max": 32
  }'
echo
echo

# get by ids
echo "7. get rows by id..."
curl -s -X POST "$BASE_URL/get" \
  -H "Content-Type: application/json" \
  -d '{
    "table": "users",
    "ids": [1, 2]
  }'
echo
echo

# update
echo "8. updating row..."
curl -s -X POST "$BASE_URL/update" \
  -H "Content-Type: application/json" \
  -d '{
    "table": "users",
    "id": 1,
    "values": [1, "alice updated", "alice.new@example.com", 31]
  }'
echo
echo

# stats
echo "9. getting stats..."
curl -s "$BASE_URL/stats"
echo
echo

# delete
echo "10. deleting rows..."
curl -s -X POST "$BASE_URL/delete" \
  -H "Content-Type: application/json" \
  -d '{
    "table": "users",
    "ids": [3]
  }'
echo
echo

# health check
echo "11. health check..."
curl -s "$BASE_URL/health"
echo
echo

echo "=== done ==="
