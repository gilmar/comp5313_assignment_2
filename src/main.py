import utils
import argparse

def main_process(input_file: str, output_file: str) -> None:
    utils.process_csv(input_file, output_file)
 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process CSV data into a NetworkX graph")
    parser.add_argument("input_file", help="Path to the input CSV file")
    parser.add_argument("output_file", help="Path to save the output graph")
    
    args = parser.parse_args()
    main_process(args.input_file, args.output_file)