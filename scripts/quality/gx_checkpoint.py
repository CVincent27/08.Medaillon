import pandas as pd
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest
from great_expectations.validator.validator import Validator
import os

# https://www.datacamp.com/tutorial/great-expectations-tutorial

SCRIPT_DIR = os.path.dirname(__file__)
nodes_csv_file = os.path.join(SCRIPT_DIR, "../../data/raw/nodes.csv")
edges_csv_file = os.path.join(SCRIPT_DIR, "../../data/raw/edges.csv")
results_nodes_files = os.path.join(SCRIPT_DIR, "./results_nodes.csv")
results_edges_files = os.path.join(SCRIPT_DIR, "./results_edges.csv")


# création du contexte Ephemeral
context = gx.get_context()
assert type(context).__name__ == "EphemeralDataContext"

def nodes_check():
    # NODES
    df_nodes = pd.read_csv(nodes_csv_file)
    if "nodes" in context.list_datasources():
        data_source_nodes = context.data_sources.get("nodes")
    else:
        data_source_nodes = context.data_sources.add_pandas(name="nodes")
    data_asset_nodes = data_source_nodes.add_dataframe_asset(name="nodes_asset")

    batch_nodes = data_asset_nodes.add_batch_definition_whole_dataframe("nodes_batch").get_batch(
        batch_parameters={"dataframe": df_nodes})

    suite_nodes = gx.ExpectationSuite(name="nodes_suite")
    suite_nodes.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="id"))
    suite_nodes.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="id"))

    validation_results_nodes = batch_nodes.validate(suite_nodes)
    print(validation_results_nodes)
    results_nodes_df = pd.DataFrame(validation_results_nodes["results"])
    results_nodes_df.to_csv(results_nodes_files, index=False)

def edges_check():
    # EDGES
    df_edges = pd.read_csv(edges_csv_file)

    if "edges" in context.list_datasources():
        data_source_edges = context.data_sources.get("edges")
    else:
        data_source_edges = context.data_sources.add_pandas(name="edges")
    data_asset_edges = data_source_edges.add_dataframe_asset(name="edges_asset")

    batch_edges = data_asset_edges.add_batch_definition_whole_dataframe("edges_batch").get_batch(
        batch_parameters={"dataframe": df_edges})

    suite_edges = gx.ExpectationSuite(name="edges_suite")
    suite_edges.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="dst"))
    suite_edges.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="src"))
    suite_edges.add_expectation(gx.expectations.ExpectColumnValuesToBeUnique(column="src"))
    
    validation_results_edges = batch_edges.validate(suite_edges)
    print(validation_results_edges)
    results_edges_df = pd.DataFrame(validation_results_edges["results"])
    results_edges_df.to_csv(results_edges_files, index=False)
