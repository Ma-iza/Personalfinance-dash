import pandas as pd
from sqlalchemy import create_engine, text

#function to extract data from the dataset and return it as a pandas dataframe
def extract():
    from datasets import load_dataset
    ds = load_dataset("Krishnan15/Personal_FinanceDataset")
    df = ds['train'].to_pandas()
    return df

#function to clean the data
def transform(df):
    df = df.copy()
    df.columns = [col.lower() for col in df.columns]
    df = df.dropna()
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[^a-z0-9]+", "_", regex=True)
        .str.replace(r"_+", "_", regex=True)
        .str.strip("_")
    )
    return df

#function to load into a database
def load(df, table_name, connection_string="sqlite:///finance.db"):
    engine = create_engine(connection_string)
    try:
        df.to_sql(
            name=table_name,
            con=engine,
            if_exists="replace",  #overwriting the table
            index=False
        )
        print(f"Data loaded into table '{table_name}' successfully.")
    except Exception as e:
        print(f"Error loading data: {e}")

#function to drop table if it exists
def drop_table(table_name, connection_string="sqlite:///finance.db"):
    engine = create_engine(connection_string)
    try:
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            print(f"Table '{table_name}' dropped successfully.")            
    except Exception as e:
        print(f"Error dropping table: {e}")
