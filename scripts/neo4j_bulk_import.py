import pandas as pd
import os
import glob

SCRIPT_DIR = os.path.dirname(__file__)
nodes_parquet_file = os.path.join(SCRIPT_DIR, "../data/silver/nodes.parquet")
edges_parquet_shards = os.path.join(SCRIPT_DIR, "../data/silver/")
gold_dir = os.path.join(SCRIPT_DIR, "../data/gold")
os.makedirs(gold_dir, exist_ok=True) 

def nodes_parquet_to_csv(nodes_parquet):
    df_nodes = pd.read_parquet(nodes_parquet)
    df_nodes.rename(columns={'id':'ID'}, inplace=True)
    df_nodes.to_csv(os.path.join(gold_dir, "nodes.csv"), index=False)

def edges_parquet_to_csv(edges_parquet):
    edges_shards = glob.glob(os.path.join(edges_parquet_shards, "shard=*", "*.parquet"))
    print(f"{len(edges_shards)} fichiers trouvés")

    # Lecture + fusion
    dfs_edges = [pd.read_parquet(f) for f in edges_shards]
    df_edges = pd.concat(dfs_edges, ignore_index=True)

    df_edges.rename(columns={'src': ':START_ID', 'dst': ':END_ID'}, inplace=True)
    print(f" edges.csv créé {len(df_edges)} lignes")
    df_edges.to_csv(os.path.join(gold_dir, 'edges.csv'), index=False)