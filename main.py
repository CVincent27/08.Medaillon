import os
from scripts.generate_data import generate_data
from scripts.convert_to_parquet import convert_to_parquet

from scripts.quality.gx_checkpoint import edges_check, nodes_check

# chemins
BASE_DIR = os.path.dirname(__file__)
RAW_DIR = os.path.join(BASE_DIR, "data", "raw")
BRONZE_DIR = os.path.join(BASE_DIR, "data", "bronze")

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
    print("pipeline terminé")

if __name__ == "__main__":
    main()
