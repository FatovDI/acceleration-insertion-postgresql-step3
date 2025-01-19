#!/bin/bash

export PGPASSWORD="postgres"


# Database connection details
DB_HOST="localhost"
DB_PORT="5437"
DB_NAME="postgres"
DB_USER="postgres"

# Number of clients (threads)
NUM_CLIENTS=10

# Number of transactions per client
NUM_TRANSACTIONS=1000

# Query file
#QUERY_FILE="query_anti_join_by_id.sql"
#QUERY_FILE="query_left_join_by_id.sql"
QUERY_FILE="query_not_in_by_id.sql"
#QUERY_FILE="query_anti_join_by_date.sql"
#QUERY_FILE="query_left_join_by_date.sql"
#QUERY_FILE="query_not_in_by_date.sql"

# Construct the pgbench command with custom query and parameters
function run_pgbench() {
  for (( i = 0; i < 3; i++ )); do
    pgbench -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER" -d "$DB_NAME" -c "$NUM_CLIENTS" -j "$NUM_CLIENTS" -t "$NUM_TRANSACTIONS" -f "$QUERY_FILE"
  done
}

run_pgbench
