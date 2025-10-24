import pandas as pd
import os

# chemin relatif
SCRIPT_DIR = os.path.dirname(__file__)
nodes_csv_file = os.path.join(SCRIPT_DIR, "../data/raw/nodes.csv")
edges_csv_file = os.path.join(SCRIPT_DIR, "../data/raw/edges.csv")
BRONZE_PATH = os.path.join(SCRIPT_DIR, "../data/bronze/")

nodes_parquet_file = os.path.join(BRONZE_PATH, "nodes.parquet")
edges_parquet_file = os.path.join(BRONZE_PATH, "edges.parquet")


def convert_to_parquet(csv_file, destination):
    df = pd.read_csv(csv_file)
    print(df.head(5))
    df.to_parquet(destination, engine="pyarrow", index=False)
    df_loaded = pd.read_parquet(destination)
    print(df_loaded)

convert_to_parquet(nodes_csv_file, nodes_parquet_file)
convert_to_parquet(edges_csv_file, edges_parquet_file)