
import matplotlib.pyplot as plt
import duckdb

CRYPTO_DATA = "./crypto.parquet"

def main() -> None:
    df = duckdb.read_parquet(CRYPTO_DATA).df()

    print(df.dtypes)
    print(df.describe())    
    print(df.head())

    df['Close'].plot()
    plt.savefig("crypto.png", format="png")


if __name__ == "__main__":
    main()
