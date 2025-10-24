import pandas as pd
import numpy as np
import os

NB_NODES = 10000
NB_EDGES = 50000
NODES_TYPE = ['Person', 'Org', 'Paper']
FILE_PATH = os.path.join(os.path.dirname(__file__), "../data/raw")
os.makedirs(FILE_PATH, exist_ok=True)


def generate_nodes(nb_nodes, nodes_type):
    ids = np.arange(nb_nodes)
    labels = np.random.choice(nodes_type, size=nb_nodes)
    names = [f"name_{i}" for i in range(nb_nodes)]
    df = pd.DataFrame({
        "id": ids,
        "label": labels,
        "name": names
    })
    #print(df.head(2))
    return df

def generate_edges(nb_edges, nb_nodes, edge_type = "REL"):
    src = np.arange(nb_edges)
    dst = np.random.randint(0, nb_nodes, size=nb_edges)
    df = pd.DataFrame({
        "src": src,
        "dst": dst,
        "type": edge_type
    })
    #print(df.head(2))
    return df

def save_df(df, filename):
    path = os.path.join(FILE_PATH, filename)
    df.to_csv(path, index=False)
    print(f"{filename} saved")

def generate_data():
    nodes_df = generate_nodes(NB_NODES, NODES_TYPE)
    save_df(nodes_df, "nodes.csv")

    edges_df = generate_edges(NB_EDGES, NB_NODES)
    save_df(edges_df, "edges.csv")

generate_data()