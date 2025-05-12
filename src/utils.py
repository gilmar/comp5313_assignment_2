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

import polars as pl
import networkx as nx
from typing import Optional

def read_csv_to_polars(file_path: str) -> pl.DataFrame:
    """
    Read a CSV file into a Polars DataFrame.
    
    Args:
        file_path (str): Path to the CSV file
        
    Returns:
        pl.DataFrame: The loaded DataFrame
    """
    df = pl.read_csv(file_path, has_header=True, try_parse_dates=True)
    df = df.with_columns(pl.col("actor_id").cast(pl.Utf8))
    return df

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
    Process a CSV file and returns a DataFrame with the processed data.
    
    Args:
        input_file (str): Path to the input CSV file
    """
    # Read CSV file
    df = read_csv_to_polars(input_file)
    print(f"DataFrame loaded with {df.shape[0]} rows and {df.shape[1]} columns.")

    return df

def process_dataframe(df: pl.DataFrame) -> pl.DataFrame:
    """
    Process the DataFrame to create a NetworkX graph.
    
    Args:
        df (pl.DataFrame): Input DataFrame containing the data to process
        
    Returns:
        pl.DataFrame: The processed DataFrame
    """
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

def filter_by_event_type(df: pl.DataFrame, event_type: Optional[str] = None) -> pl.DataFrame:
    """
    Filter the DataFrame to include only events of a specified type.
    
    Args:
        df (pl.DataFrame): Input DataFrame containing the event data
        event_type (Optional[str]): The event type to filter by. If None, returns the original DataFrame.
        
    Returns:
        pl.DataFrame: DataFrame filtered by the specified event type
    """
    if event_type is None:
        return df

    return df.filter(pl.col("object_type") == event_type)

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
        df = filter_by_event_type(df, event_type)
    
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

def create_graphs_by_date(df: pl.DataFrame) -> dict[str, nx.Graph]:
    """
    Create a NetworkX graph for each unique date in the DataFrame.
    
    Args:
        df (pl.DataFrame): Input DataFrame containing graph data
        
    Returns:
        dict[str, nx.Graph]: Dictionary mapping dates to their corresponding graphs
    """
    # Group data by event_date
    graphs_by_date: dict[str, nx.Graph] = {}

    # Get unique dates
    unique_dates = df.select("event_date").unique().sort("event_date")
    
    for date in unique_dates["event_date"]:
        # Filter data for this date
        date_df = df.filter(pl.col("event_date") == date)
        
        # Create graph for this date
        g: nx.Graph = nx.Graph()
        
        # Add edges for this date
        for row in date_df.iter_rows():
            _, actor_id, actor_id_2, total_daily_event_count, _ = row
            g.add_edge(actor_id, actor_id_2, weight=total_daily_event_count)
        
        # Store graph with the date as key
        graphs_by_date[date.strftime("%Y-%m-%d")] = g
    
    return graphs_by_date

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
    }
    return metrics

def graphs_metrics_to_dataframe(graphs_by_date: dict[str, nx.Graph]) -> pl.DataFrame:
    """
    Convert a dictionary of graphs by date into a Polars DataFrame with graph metrics.
    
    Args:
        graphs_by_date (dict[str, nx.Graph]): Dictionary mapping dates to their corresponding graphs.
        
    Returns:
        pl.DataFrame: A DataFrame with dates and graph metrics as columns.
    """
    metrics_list = []

    for date, graph in graphs_by_date.items():
        metrics = calculate_graph_metrics(graph)
        metrics["event_date"] = date
        metrics_list.append(metrics)

    return pl.DataFrame(metrics_list)