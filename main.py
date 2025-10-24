import os
from scripts.generate_data import generate_data
from scripts.convert_to_parquet import convert_to_parquet
from scripts.quality.gx_checkpoint import edges_check, nodes_check
from scripts.partition_edges import del_old_shard, partitioning_edges, copy_nodes, df_edges
from scripts.neo4j_bulk_import import nodes_parquet_to_csv, edges_parquet_to_csv, edges_parquet_shards, nodes_parquet_file

# chemins
BASE_DIR = os.path.dirname(__file__)
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")
SILVER_DIR = os.path.join(BASE_DIR, "data", "silver")

nodes_csv = os.path.join(RAW_DIR, "nodes.csv")
edges_csv = os.path.join(RAW_DIR, "edges.csv")

nodes_parquet = os.path.join(BRONZE_DIR, "nodes.parquet")
edges_parquet = os.path.join(BRONZE_DIR, "edges.parquet")

def main():
    print("Lancement pipeline")
    
    generate_data()
    convert_to_parquet(nodes_csv, nodes_parquet)
    convert_to_parquet(edges_csv, edges_parquet)
    nodes_check()
    edges_check()
    del_old_shard()
    partitioning_edges(df_edges)
    copy_nodes()
    nodes_parquet_to_csv(nodes_parquet_file)
    edges_parquet_to_csv(edges_parquet_shards)

    print("pipeline terminé")

if __name__ == "__main__":
    main()
