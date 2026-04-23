import pandas as pd
from sqlalchemy import create_engine, text

#function to extract data from the dataset and return it as a pandas dataframe
def extract():
    from datasets import load_dataset
    ds = load_dataset("Krishnan15/Personal_FinanceDataset")
    df = ds['train'].to_pandas()
    return df

#function to clean the data
def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(r"[^a-z0-9]+", "_", regex=True)
        .str.replace(r"_+", "_", regex=True)
        .str.strip("_")
    )
    return df

def transform(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df = clean_column_names(df)
    #Rename to better names
    rename_map = {
        "paymentmethod": "payment_method",
        "accounttype": "account_type",
        "deviceused": "device_used",
        "merchanttype": "merchant_type",
        "loyaltyprogram": "loyalty_program",
        "timeofday": "time_of_day",
        "month": "month_name",  #to avoid collision with numeric month field
    }
    df = df.rename(columns=rename_map)

    #standardize string columns
    text_cols = [
        "category",
        "payment_method",
        "account_type",
        "transaction_type",
        "location",
        "device_used",
        "merchant_type",
        "loyalty_program",
        "weekday",
        "month_name",
        "time_of_day",
    ]
    for col in text_cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Date normalization
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        # keeps original timestamps,but store a clean date string for SQLite
        df["date"] = df["date"].dt.strftime("%Y-%m-%d")

    #amount normalization
    if "amount" in df.columns:
        df["amount"] = (
            df["amount"]
            .astype(str)
            .str.replace(",", "", regex=False)
        )
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

    if "transaction_type" in df.columns:
        df["transaction_type"] = df["transaction_type"].str.lower()
        df["type"] = df["transaction_type"].map({"debit": "expense"}).fillna("expense")
    #Droprows
    df = df.dropna(subset=["date", "amount", "category"])
    df = df.drop_duplicates()
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
