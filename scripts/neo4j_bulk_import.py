import pandas as pd
import os

SCRIPT_DIR = os.path.dirname(__file__)
nodes_parquet_file = os.path.join(SCRIPT_DIR, "../data/silver/nodes.parquet")
edges_parquet_file = os.path.join(SCRIPT_DIR, "../data/bronze/edges.parquet")

gold_dir = os.path.join(SCRIPT_DIR, "../data/gold")
os.makedirs(gold_dir, exist_ok=True) 

df_nodes = pd.read_parquet(nodes_parquet_file)
df_nodes.rename(columns={'id':'ID'}, inplace=True)

df_edges = pd.read_parquet(edges_parquet_file)
df_edges.rename(columns={'src':':START_ID', 'dst':':END_ID'}, inplace=True)

print(df_nodes)
print(df_edges)

df_nodes.to_csv(os.path.join(gold_dir, "nodes.csv"), index=False)
df_edges.to_csv(os.path.join(gold_dir, "edges.csv"), index=False)