import os
import pandas as pd
import shutil
import glob

SCRIPT_DIR = os.path.dirname(__file__)
edges_bronze_file = os.path.join(SCRIPT_DIR, "../data/bronze/edges.parquet")
edges_silver_file = os.path.join(SCRIPT_DIR, "../data/silver/")

nodes_bronze_file = os.path.join(SCRIPT_DIR, "../data/bronze/nodes.parquet")
nodes_silver_file = os.path.join(SCRIPT_DIR, "../data/silver/")

df_edges = pd.read_parquet(edges_bronze_file)
df_nodes = pd.read_parquet(nodes_bronze_file)

def del_old_shard():
    for shard_dir in glob.glob(os.path.join(edges_silver_file, "shard=*")):
        shutil.rmtree(shard_dir)
    print(f"Supprimé : {shard_dir}")

def partitioning_edges(df):
    # add col shard (modulo 8 = 8 shard)
    df['shard'] = df.index % 8 
    #print(df)
    df.to_parquet(edges_silver_file, partition_cols=['shard'], engine='pyarrow', index=False)

def copy_nodes():
    shutil.copy2(nodes_bronze_file, nodes_silver_file)
    
del_old_shard()
partitioning_edges(df_edges)
copy_nodes()