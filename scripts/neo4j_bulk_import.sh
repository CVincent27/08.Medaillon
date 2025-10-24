#!/bin/bash

NEO4J_USER=neo4j
NEO4J_PASS=vincentc27test
NEO4J_BOLT=bolt://localhost:7687

SILVER_DIR="./data/silver"
GOLD_DIR="./data/gold"

mkdir -p $GOLD_DIR

# parquet -> CSV
convert_nodes() {
    local parquet_file=$1
    local csv_file=$2
    python3 - <<END
import pandas as pd
df = pd.read_parquet("$parquet_file")
df = df.rename(columns={"id":"id:ID","name":"name","label":"label"})
df.to_csv("$csv_file", index=False)
END
}

convert_edges() {
    local parquet_file=$1
    local csv_file=$2
    python3 - <<END
import pandas as pd
df = pd.read_parquet("$parquet_file")
df = df.rename(columns={"start_id":":START_ID","end_id":":END_ID","relation":"type"})
df.to_csv("$csv_file", index=False)
END
}

echo "==> Conversion Nodes"
for f in $SILVER_DIR/*_nodes.parquet; do
    filename=$(basename "$f" .parquet)
    convert_nodes "$f" "$GOLD_DIR/${filename}.csv"
done

echo "==> Conversion Edges"
for f in $SILVER_DIR/*_edges.parquet; do
    filename=$(basename "$f" .parquet)
    convert_edges "$f" "$GOLD_DIR/${filename}.csv"
done

# Import CSV -> Neo4j
echo "==> Import des Nœuds"
for f in $GOLD_DIR/*_nodes.csv; do
    echo "Import $f"
    cypher-shell -u $NEO4J_USER -p $NEO4J_PASS -a $NEO4J_BOLT "LOAD CSV WITH HEADERS FROM 'file:///$f' AS row
    CREA
