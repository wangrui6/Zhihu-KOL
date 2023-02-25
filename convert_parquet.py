import pandas as pd


if __name__ == "__main__":
    input_csv = 'nicole-97-93.csv'
    # Create a pandas dataframe from your dataset file(s)
    df = pd.read_csv(input_csv) # or any other way

    # Save the file in the Parquet format
    df.to_parquet("dataset.parquet", row_group_size=100, engine="pyarrow")