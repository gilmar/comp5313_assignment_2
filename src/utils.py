#!/usr/bin/env python3
"""
Utilities for Graph Processing and Data Analysis

This script provides utility functions for processing CSV data into Polars DataFrames,
performing data transformations, generating graphs using NetworkX, and exporting the results.

Key functionalities include:
- Reading CSV files into Polars DataFrames
- Processing data to calculate actor pairs and monthly statistics
- Creating and exporting NetworkX graphs

Usage:
    Import the required functions from this script into your project:
        from utils import read_csv_to_polars, process_dataframe, create_graph_from_dataframe, export_graph

    Example:
        df = read_csv_to_polars("input.csv")
        processed_df = process_dataframe(df)
        graph = create_graph_from_dataframe(processed_df)
        export_graph(graph, "output.graphml")
"""

from typing import Optional, List, Tuple, Dict
import os
from datetime import datetime
from glob import glob
import polars as pl
import networkx as nx
import dcor  # type: ignore

def _parse_mixed_datetime(dt_str):
    datetime_formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%z",  # Handles ISO8601 with timezone, e.g., 2014-09-08T11:55:49-07:00
        "%Y/%m/%d %H:%M:%S %z",  # Handles e.g., 2012/05/01 10:25:30 -0700
    ]
    for fmt in datetime_formats:
        try:
            return datetime.strptime(dt_str, fmt).date()
        except (ValueError, TypeError):
            continue
    return None

def read_csv_to_polars(file_path: str) -> pl.DataFrame:
    """
    Read a CSV file into a Polars DataFrame with a specified schema.
    Adds a new 'event_date' column parsed from 'event_date_str' using _parse_mixed_datetime.
    
    Args:
        file_path (str): Path to the CSV file
        
    Returns:
        pl.DataFrame: The loaded DataFrame
    """
    schema = {
        "repository": pl.Utf8,
        "event_date_str": pl.Utf8,
        "action": pl.Utf8,
        "object_type": pl.Utf8,
        "actor_id": pl.Utf8,
        "object_id": pl.Utf8,
    }
    df = pl.read_csv(
        file_path,
        has_header=False,
        try_parse_dates=False,
        separator=",",
        encoding="utf8",
        infer_schema_length=1000,
        new_columns=list(schema.keys()),
        schema=schema
    )

    df = df.with_columns(
        pl.col("event_date_str").map_elements(_parse_mixed_datetime, return_dtype=pl.Date).alias("event_date")
    ).drop("event_date_str")

    df = df.with_columns(
        pl.when(pl.col("object_type").str.to_lowercase().str.starts_with("issue"))
        .then(pl.lit("issue"))
        .otherwise(pl.lit("pr"))
        .alias("object_type")
    )
    
    return df

def read_and_concat_csvs_from_dir(
    directory: str,
    repositories: Optional[list[str]] = None
) -> pl.DataFrame:
    """
    Read all CSV files from a directory, optionally filter for only the specified repositories,
    and concatenate the records into a single DataFrame.

    Args:
        directory (str): Path to the directory containing CSV files.
        repositories (list[str], optional): List of repository names to include. If None, include all.

    Returns:
        pl.DataFrame: Concatenated DataFrame, optionally filtered by repositories.
    """
    csv_files = glob(os.path.join(directory, "*.csv.gz"))
    dfs = []
    for file in csv_files:
        df = read_csv_to_polars(file)
        if repositories is not None:
            df = df.filter(pl.col("repository").is_in(repositories))
        dfs.append(df)
    if dfs:
        return pl.concat(dfs)
    else:
        return pl.DataFrame([])

def daily_event_count(
    df: pl.DataFrame
) -> pl.DataFrame:
    """
    Given a DataFrame, return a DataFrame with daily_event_count grouped by
    event_date, actor_id, object_id, and object_type.

    Args:
        df (pl.DataFrame): Input DataFrame (e.g., from read_and_concat_csvs_from_dir)

    Returns:
        pl.DataFrame: Grouped DataFrame with daily_event_count
    """
    return (
        df.group_by(["event_date", "actor_id", "object_id", "object_type"])
          .agg(pl.count().alias("daily_event_count"))
          .sort(["event_date", "actor_id", "object_id", "object_type"])
    )

def export_graph(graph: nx.Graph, output_path: str) -> None:
    """
    Export a NetworkX graph to a file.
    
    Args:
        graph (nx.Graph): The graph to export
        output_path (str): Path where to save the graph
    """
    nx.write_graphml(graph, output_path)


def process_csv(input_file: str) -> pl.DataFrame:
    """
    [DEPRECATED] Process a CSV file and returns a DataFrame with the processed data.
    
    Args:
        input_file (str): Path to the input CSV file
    """
    # Deprecated: Use read_csv_to_polars directly instead.
    df = read_csv_to_polars(input_file)
    print(f"DataFrame loaded with {df.shape[0]} rows and {df.shape[1]} columns.")

    return df

def _create_actor_pairs_by_date_and_object(df: pl.DataFrame) -> pl.DataFrame:

    # Create all possible actor pairs for each date and object
    actor_pairs = df.join(
        df,
        on=["event_date", "object_id"],
        how="inner",
        suffix="_2"
    ).filter(
        pl.col("actor_id") < pl.col("actor_id_2")  # Avoid duplicate pairs (A,B) and (B,A)
    ).select([
        "event_date",
        "object_id", 
        "actor_id",
        "actor_id_2",
        (pl.col("daily_event_count") + pl.col("daily_event_count_2")).alias("total_daily_event_count")
    ])

    return actor_pairs

def prepare_df_to_graph(df: pl.DataFrame, event_type: Optional[str] = None) -> pl.DataFrame:
    """
    Process the DataFrame to create a NetworkX graph.
    
    Args:
        df (pl.DataFrame): Input DataFrame containing the data to process
        
    Returns:
        pl.DataFrame: The processed DataFrame
    """

    # Filter by event type if specified
    if event_type is not None:
        df = df.filter(pl.col("object_type") == event_type)

    actor_pairs = _create_actor_pairs_by_date_and_object(df)

    # Group by date and actor pairs, summing the total daily event count
    actor_pairs = actor_pairs.group_by(
        ["event_date", "actor_id", "actor_id_2"]
    ).agg(
        pl.col("total_daily_event_count").sum().alias("total_daily_event_count")
    ).sort(
        ["event_date"]
    )

    return actor_pairs

def rolling_window_stats(df: pl.DataFrame, event_type: Optional[str] = None, window_days: int = 30) -> pl.DataFrame:
    """
    Calculate statistics using a rolling window with a specified number of days.
    
    Args:
        df (pl.DataFrame): Input DataFrame containing the data to process
        window_days (int): Size of the rolling window in days
        event_type (str, optional): Filter data by this event type before calculation
        
    Returns:
        pl.DataFrame: The DataFrame with rolling window statistics
    """
    # Filter by event type if specified
    if event_type is not None:
        df = df.filter(pl.col("object_type") == event_type)
    
    # Sort by event_date to ensure proper window calculation
    sorted_df = df.sort("event_date")
    
    # Group by date first to get daily stats
    daily_stats = sorted_df.group_by("event_date").agg(
        pl.col("actor_id").n_unique().alias("daily_unique_actors"),
        pl.col("daily_event_count").sum().alias("daily_event_count")
    )
    
    # Apply rolling window
    rolling_stats = daily_stats.select([
        "event_date",
        pl.col("daily_unique_actors").rolling_sum(window_size=window_days).alias("rolling_unique_actors"),
        pl.col("daily_event_count").rolling_sum(window_size=window_days).alias("rolling_event_count")
    ])
    
    return rolling_stats.sort("event_date")



def create_graphs_by_date(
    df: pl.DataFrame
) -> dict[pl.Date, nx.Graph]:
    """
    Create a NetworkX graph for each unique date in the DataFrame, optionally filtering by event_type.
    
    Args:
        df (pl.DataFrame): Input DataFrame containing graph data
        
    Returns:
        dict[str, nx.Graph]: Dictionary mapping dates to their corresponding graphs
    """
    graphs_by_date: dict[pl.Date, nx.Graph] = {}

    unique_dates = df.select("event_date").unique().sort("event_date")
    
    for date in unique_dates["event_date"]:
        date_df = df.filter(pl.col("event_date") == date)
        g: nx.Graph = nx.Graph()
        for row in date_df.iter_rows():
            _, actor_id, actor_id_2, total_daily_event_count = row
            g.add_edge(actor_id, actor_id_2, weight=total_daily_event_count)
        graphs_by_date[date] = g
    
    return graphs_by_date

def count_supernodes_by_degree(G: nx.Graph) -> int:
    df = pl.DataFrame(
        [(n, d) for n, d in G.degree()],
        schema=[("node", pl.Utf8), ("degree", pl.Int64)],
        strict=False
    )
    stats = df.select([
        pl.col("degree").mean().alias("mean"),
        pl.col("degree").std().alias("std")
    ]).row(0)

    threshold = stats[0] + 2 * stats[1]
    return df.filter(pl.col("degree") > threshold).height


def count_supernodes_by_betweenness(G: nx.Graph) -> int:
    betweenness = nx.betweenness_centrality(G)
    df = pl.DataFrame({
        "node": list(betweenness.keys()),
        "betweenness": list(betweenness.values())
    })

    stats = df.select([
        pl.col("betweenness").mean().alias("mean"),
        pl.col("betweenness").std().alias("std")
    ]).row(0)

    threshold = stats[0] + 2 * stats[1]
    return df.filter(pl.col("betweenness") > threshold).height


def count_supernodes_by_eigenvector(G: nx.Graph) -> int:
    eigen = nx.eigenvector_centrality(G, max_iter=1000)
    df = pl.DataFrame({
        "node": list(eigen.keys()),
        "eigenvector": list(eigen.values())
    })

    stats = df.select([
        pl.col("eigenvector").mean().alias("mean"),
        pl.col("eigenvector").std().alias("std")
    ]).row(0)

    threshold = stats[0] + 2 * stats[1]
    return df.filter(pl.col("eigenvector") > threshold).height

#number of connected components
def count_connected_components(G: nx.Graph) -> int:
    """
    Count the number of connected components in a graph.
    
    Args:
        G (nx.Graph): The graph to analyze.
        
    Returns:
        int: The number of connected components in the graph.
    """
    return nx.number_connected_components(G)

def calculate_graph_metrics(graph: nx.Graph) -> dict:
    """
    Calculate common graph metrics for a given NetworkX graph.
    
    Args:
        graph (nx.Graph): The graph to analyze.
        
    Returns:
        dict: A dictionary containing the calculated metrics.
    """
    metrics = {
        "number_of_nodes": graph.number_of_nodes(),
        "number_of_edges": graph.number_of_edges(),
        "average_clustering": nx.average_clustering(graph),
        "density": nx.density(graph),
        "average_degree": sum(dict(graph.degree()).values()) / graph.number_of_nodes() if graph.number_of_nodes() > 0 else 0,
        "number_of_isolated_nodes": sum(1 for node, degree in dict(graph.degree()).items() if degree == 0),
        "number_of_connected_components":  nx.number_connected_components(graph),
        "number_of_supernodes_by_degree": count_supernodes_by_degree(graph),
        "number_of_supernodes_by_betweenness": count_supernodes_by_betweenness(graph),
        #"number_of_supernodes_by_eigenvector": count_supernodes_by_eigenvector(graph),
        "average_shortest_path_length": nx.average_shortest_path_length(graph) if nx.is_connected(graph) else float('inf'),
        "diameter": nx.diameter(graph) if nx.is_connected(graph) else float('inf'),
    }
    return metrics

def graphs_metrics_to_dataframe(
    graphs_by_date: dict[pl.Date, nx.Graph],
    smoothing_period_days: int = 0
) -> pl.DataFrame:
    """
    Convert a dictionary of graphs by date into a Polars DataFrame with graph metrics.
    Optionally smooth metrics using a rolling average over the specified period (in days).
    
    Args:
        graphs_by_date (dict[str, nx.Graph]): Dictionary mapping dates to their corresponding graphs.
        smoothing_period_days (int, optional): Number of days for rolling average smoothing. Default is 0 (no smoothing).
        
    Returns:
        pl.DataFrame: A DataFrame with dates and graph metrics as columns.
    """
    metrics_list = []

    for date, graph in graphs_by_date.items():
        metrics = calculate_graph_metrics(graph)
        metrics["event_date"] = date
        metrics_list.append(metrics)

    df = pl.DataFrame(metrics_list).sort("event_date")

    if smoothing_period_days > 1:
        metric_cols = [col for col in df.columns if col != "event_date"]
        for col in metric_cols:
            df = df.with_columns(
                pl.col(col)
                .rolling_mean(window_size=smoothing_period_days, min_samples=1)
                .alias(col)
            )

    return df

def distance_correlation_with_column(df: pl.DataFrame, target_col: str) -> pl.DataFrame:
    """
    Compute non-lagged distance correlation between a specified column and all other numeric columns,
    excluding self-comparison.

    Args:
        df (pl.DataFrame): Input DataFrame.
        target_col (str): The column to compare with all others.

    Returns:
        Polars DataFrame with columns: col_x, col_y, dcor
    """
    numeric_cols = [col for col, dtype in zip(df.columns, df.dtypes) if dtype.is_numeric()]
    if target_col not in numeric_cols:
        raise ValueError(f"Column '{target_col}' is not numeric or not found in DataFrame.")

    records = []
    x_np = df[target_col].to_numpy()
    for col in numeric_cols:
        if col == target_col:
            continue
        y_np = df[col].to_numpy()
        d = dcor.distance_correlation(x_np, y_np)
        records.append((target_col, col, d))

    return pl.DataFrame(records, schema=["col_x", "col_y", "dcor"])

def distance_correlation_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """
    Compute non-lagged distance correlation between all numeric column pairs.

    Returns:
        Polars DataFrame with columns: col_x, col_y, dcor
    """
    numeric_cols = [col for col, dtype in zip(df.columns, df.dtypes) if dtype.is_numeric()]
    records = []

    for i in range(len(numeric_cols)):
        for j in range(i + 1, len(numeric_cols)):
            col_x, col_y = numeric_cols[i], numeric_cols[j]
            x_np = df[col_x].to_numpy()
            y_np = df[col_y].to_numpy()
            d = dcor.distance_correlation(x_np, y_np)
            records.append((col_x, col_y, d))
            records.append((col_y, col_x, d))  # symmetric

    for col in numeric_cols:
        records.append((col, col, 1.0))  # self-correlation

    return pl.DataFrame(records, schema=["col_x", "col_y", "dcor"])

def lagged_distance_correlation_all_pairs(
    df: pl.DataFrame,
    max_lag: int = 20
) -> Dict[Tuple[str, str], List[Tuple[int, float]]]:
    """
    Compute lagged distance correlation for all unique pairs of numeric columns in a Polars DataFrame.

    Parameters:
        df (pl.DataFrame): The input DataFrame.
        max_lag (int): The maximum lag (positive and negative) to evaluate.

    Returns:
        Dictionary mapping (col_x, col_y) -> list of (lag, distance_correlation)
    """
    numeric_columns = [col for col, dtype in zip(df.columns, df.dtypes) if dtype.is_numeric()]
    results = {}

    for i in range(len(numeric_columns)):
        for j in range(i + 1, len(numeric_columns)):
            col_x = numeric_columns[i]
            col_y = numeric_columns[j]

            x_np = df[col_x].to_numpy()
            y_np = df[col_y].to_numpy()

            pair_results = []
            for lag in range(-max_lag, max_lag + 1):
                if lag < 0:
                    d = dcor.distance_correlation(x_np[:lag], y_np[-lag:])
                elif lag > 0:
                    d = dcor.distance_correlation(x_np[lag:], y_np[:-lag])
                else:
                    d = dcor.distance_correlation(x_np, y_np)
                pair_results.append((lag, d))

            results[(col_x, col_y)] = pair_results

    return results

from typing import Dict, Tuple, List

def best_lagged_distance_correlation_per_pair(
    df: pl.DataFrame,
    max_lag: int = 20
) -> Dict[Tuple[str, str], Tuple[int, float]]:
    """
    Compute the best (maximum) lagged distance correlation for all unique column pairs.

    Parameters:
        df (pl.DataFrame): Input Polars DataFrame.
        max_lag (int): Max lag (positive and negative) to consider.

    Returns:
        Dictionary mapping (col_x, col_y) -> (best_lag, max_distance_correlation)
    """
    all_lagged_results = lagged_distance_correlation_all_pairs(df, max_lag=max_lag)

    best_results = {
        pair: max(lagged_corrs, key=lambda t: t[1])  # choose lag with highest dCor
        for pair, lagged_corrs in all_lagged_results.items()
    }

    return best_results
