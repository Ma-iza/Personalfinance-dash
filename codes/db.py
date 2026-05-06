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
    df = df.dropna()
    df['Amount'] = df['Amount'] / 10
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
    df.to_sql("transactions", engine, if_exists="replace", index=False)

    budgets = pd.DataFrame({
        'category': ['Food', 'Transport', 'Entertainment', 'Shopping'],
        'budget': [50000, 20000, 30000, 40000]
    })

    budgets.to_sql("budgets", engine, if_exists="replace", index=False)

#function to drop table if it exists
def drop_table(table_name, connection_string="sqlite:///finance.db"):
    engine = create_engine(connection_string)
    try:
        with engine.connect() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            print(f"Table '{table_name}' dropped successfully.")            
    except Exception as e:
        print(f"Error dropping table: {e}")

if __name__ == "__main__":
    df = extract()
    clean_df = transform(df)
    load(clean_df, "transactions")
