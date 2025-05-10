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


def monthly_stats(df: pl.DataFrame) -> pl.DataFrame:
    """
    Calculate monthly statistics from the DataFrame.
    
    Args:
        df (pl.DataFrame): Input DataFrame containing the data to process
        
    Returns:
        pl.DataFrame: The DataFrame with monthly statistics
    """
    # Convert event_date to month-year format
    df = df.with_columns(
        pl.col("event_date").dt.strftime("%Y-%m").alias("month_year")
    )

    # Count the number of unique actors per month
    monthly_contributors = df.group_by(
        "month_year"
    ).agg(
        pl.col("actor_id").n_unique().alias("unique_actors_count")
    ).sort(
        ["month_year"]
    )

    # Group by month-year and object type (prs or issues), summing the total daily event count
    monthly_events = df.group_by(
        ["month_year", "object_type"]
    ).agg(
        pl.col("daily_event_count").sum().alias("monthly_event_count")
    ).sort(
        ["month_year"]
    )

    return  monthly_contributors

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

def create_graph_from_dataframe(df: pl.DataFrame) -> nx.Graph:
    """
    Create a NetworkX graph from a Polars DataFrame.
    
    Args:
        df (pl.DataFrame): Input DataFrame containing graph data
        
    Returns:
        nx.Graph: The created NetworkX graph
    """
    G = nx.Graph()
    # TODO: Implement graph creation logic based on your specific requirements
    return G


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
